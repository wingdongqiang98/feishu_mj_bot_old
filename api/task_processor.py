# coding:utf-8

import json
import os
import queue
import time
from multiprocessing import Process, Queue
import concurrent.futures
from dotenv import load_dotenv


from api.utils.feishu_api import FeiShuAPI
from api.utils.log_utils import init_env
from api.utils.media_utils import download_image_io
from api.utils.task_api import MJApi
from api.utils.variables import LOGGER, CARD_MSG_TEMPLATE
from api.utils.func_utils import error_cap
load_dotenv()
task_queue = Queue()

APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
VERIFICATION_TOKEN = os.getenv("FEISHU_VERIFICATION_TOKEN")
ENCRYPT_KEY = os.getenv("FEISHU_ENCRYPT_KEY")
MAX_THREAD_NUM = int(os.getenv("MAX_THREAD_NUM", 5))
feishu_api = FeiShuAPI(APP_ID, APP_SECRET)

@error_cap()
def send_text_msg(msg, user):
    LOGGER.info("will send msg %s to user %s", msg, user)
    print("********", repr(msg), repr(user))
    access_token = feishu_api.get_tenant_access_token()["tenant_access_token"]
    feishu_api.set_access_token(access_token)
    feishu_api.send_message(user, json.dumps({"text": msg}), msg_type="text")


def process_task(task_params, task_type, task_id, user_id):
    try:
        init_env(filename="feishu_mj_bot_thread.log")

        api = MJApi(os.getenv("MJ_TASK_APIKEY"))
        task_params = json.loads(task_params)
        LOGGER.info("task %s params %s", task_id, task_params)
        if task_type == "imagine":
            mj_task_id = api.create_task(**task_params)["data"]["task_id"]
        elif task_type in ["upscale", "variation"]:
            mj_task_id = api.child_task(**task_params)["data"]["task_id"]
        else:
            raise Exception("not support task type %s" % task_type)
        timeout = int(os.getenv("TASK_TIMEOUT", "600"))
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                send_text_msg("timeout", user_id)
                break
            result = api.query_task(mj_task_id)
            status = result["data"]["status"]
            if result["data"]["status"] == "finished":
                access_token = feishu_api.get_tenant_access_token()["tenant_access_token"]
                feishu_api.set_access_token(access_token)
                image_url = result["data"]["image_url"]
                image_io = download_image_io(image_url)
                image_key = feishu_api.upload_image(image_io, os.path.basename(image_url))["data"]["image_key"]
                if task_type == "upscale":
                    feishu_api.send_message(user_id, "{\"image_key\": \"%s\"}" % image_key)
                else:
                    msg = CARD_MSG_TEMPLATE.replace("${img_key}", image_key).replace("${task_id}", mj_task_id)
                    feishu_api.send_message(user_id, msg, msg_type="interactive")
                break
            if result["data"]["status"] == "error":
                msg = result.get("msg", "")
                send_text_msg(msg, user_id)
                break
            time.sleep(1)
    except Exception as e:
        send_text_msg(str(e), user_id)


def process_tasks():
    init_env()
    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            while True:
                try:
                    task = task_queue.get_nowait()
                except queue.Empty:
                    time.sleep(1)
                    continue
                # 执行任务
                future = executor.submit(process_task, **task)


def main():
    p = Process(target=process_tasks)
    p.start()
    p.join()


if __name__ == "__main__":
    main()
