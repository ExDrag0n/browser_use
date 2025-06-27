# import asyncio
# import json
# import logging
# from uuid import uuid4
# from fastapi import FastAPI, UploadFile, Form, HTTPException
# from fastapi.responses import JSONResponse
# from datetime import datetime
# import redis.asyncio as redis
# from browser_use import Agent, Controller
# from browser_use.browser import BrowserSession
# from langchain_ollama import ChatOllama
#
#
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
#
# app = FastAPI()
# redis_client = redis.Redis()
#
# QUEUE_NAME = "BROWSER_TASK_QUEUE"
# START_DATE_KEY = "cdp_start_date"
#
#
# queue_locks = {}
#
# def get_queue_lock(cdp_url: str, day: int) -> asyncio.Lock:
#     """Возвращает или создаёт блокировку для конкретной очереди (cdp_url:day)."""
#     queue_key = f"{cdp_url}:{day}"
#     if queue_key not in queue_locks:
#         queue_locks[queue_key] = asyncio.Lock()
#     return queue_locks[queue_key]
#
#
# def parse_txt_to_tasks(file_content: str, cdp_url: str) -> list:
#     lines = file_content.strip().split("\n")
#     current_day = None
#     tasks = []
#
#     for line in lines:
#         line = line.strip()
#         if line.isdigit():
#             current_day = int(line)
#         elif line and current_day is not None:
#             for t in line.split(";"):
#                 task = {
#                     "task_id": str(uuid4()),
#                     "day": current_day,
#                     "instruction": t.strip(),
#                     "cdp_url": cdp_url,
#                     "retries_left": 3
#                 }
#                 tasks.append(task)
#                 logger.debug(f"Parsed task: {task}")
#     logger.info(f"Parsed {len(tasks)} tasks for cdp_url {cdp_url}")
#     return tasks
#
#
# @app.post("/upload_tasks/")
# async def upload_tasks(file: UploadFile, cdp_url: str = Form(...)):
#     content = (await file.read()).decode("utf-8")
#     tasks = parse_txt_to_tasks(content, cdp_url)
#
#     today = datetime.today().date()
#     await redis_client.set(f"{START_DATE_KEY}:{cdp_url}", today.isoformat())
#     logger.info(f"Set start date for CDP session {cdp_url} to {today}")
#
#     for task in tasks:
#         queue_name = f"{QUEUE_NAME}:{cdp_url}:{task['day']}"
#         await redis_client.rpush(queue_name, json.dumps(task))
#
#     return JSONResponse({"status": "ok", "tasks_queued": len(tasks)})
#
#
# async def run_browser_task(task):
#
#     user_task = task["instruction"]
#     cdp_url = task["cdp_url"]
#
#     logger.info(f"Запуск задачи {task['task_id']} через CDP: {cdp_url}")
#
#     browser_session = BrowserSession(headless=False, cdp_url=cdp_url)
#     controller = Controller()
#     agent = Agent(
#         task=user_task,
#         llm=ChatOllama(model="qwen2.5:32b", num_ctx=32000),
#         tool_calling_method='raw',
#         controller=controller,
#         browser_session=browser_session,
#     )
#
#     try:
#         result = await agent.run()
#     finally:
#         await browser_session.close()
#         logger.info(f"Сессия закрыта для {cdp_url}")
#
#     try:
#         result_str = json.dumps(result, ensure_ascii=False, default=str)
#     except Exception as e:
#         result_str = f"Failed to serialize result: {str(e)}"
#
#     await redis_client.set(f"result:{task['task_id']}", result_str)
#     logger.info(f"Результат сохранён: task_id={task['task_id']}")
#
#
# async def handle_task(task, semaphore):
#     queue_lock = get_queue_lock(task['cdp_url'], task['day'])
#     async with semaphore:
#         async with queue_lock:
#             try:
#                 async with asyncio.timeout(300):
#                     await run_browser_task(task)
#             except Exception as e:
#                 task['retries_left'] -= 1
#                 if task['retries_left'] > 0:
#                     queue_name = f"{QUEUE_NAME}:{task['cdp_url']}:{task['day']}"
#                     await redis_client.rpush(queue_name, json.dumps(task))
#                     logger.warning(
#                         f"Retrying task {task['task_id']} (cdp_url: {task['cdp_url']}, "
#                         f"day: {task['day']}, instruction: {task['instruction'][:50]}...) "
#                         f"— retries left: {task['retries_left']}"
#                     )
#                 else:
#                     await redis_client.set(f"failed:{task['task_id']}", json.dumps({
#                         "task": task,
#                         "error": str(e),
#                         "failed_at": datetime.now().isoformat()
#                     }))
#                     logger.error(
#                         f"Task {task['task_id']} (cdp_url: {task['cdp_url']}, day: {task['day']}) "
#                         f"failed permanently: {e}"
#                     )
#
# async def worker_for_cdp(cdp_url: str, semaphore):
#     while True:
#         start_date_str = await redis_client.get(f"{START_DATE_KEY}:{cdp_url}")
#         if not start_date_str:
#             await asyncio.sleep(10)
#             continue
#
#         try:
#             start_date = datetime.fromisoformat(start_date_str.decode()).date()
#         except:
#             await asyncio.sleep(10)
#             continue
#
#         today = datetime.today().date()
#         day_num = (today - start_date).days + 1
#         queue_name = f"{QUEUE_NAME}:{cdp_url}:{day_num}"
#
#         task_json = await redis_client.lpop(queue_name)
#         if not task_json:
#             await asyncio.sleep(5)
#             continue
#
#         task = json.loads(task_json)
#         await handle_task(task, semaphore)
#
# async def discover_cdp_urls():
#     keys = await redis_client.keys(f"{START_DATE_KEY}:*")
#     return [key.decode().split(":", 1)[1] for key in keys]
#
# async def start_workers(max_concurrent_tasks: int = 10):
#     semaphore = asyncio.Semaphore(max_concurrent_tasks)
#     while True:
#         cdp_urls = await discover_cdp_urls()
#         for cdp_url in cdp_urls:
#             asyncio.create_task(worker_for_cdp(cdp_url, semaphore))
#         await asyncio.sleep(60)
#
# @app.get("/user_status/")
# async def get_user_status(cdp_url: str):
#     start_date_str = await redis_client.get(f"{START_DATE_KEY}:{cdp_url}")
#     if not start_date_str:
#         raise HTTPException(status_code=404, detail="CDP URL не найден")
#
#     start_date = datetime.fromisoformat(start_date_str.decode()).date()
#     today = datetime.today().date()
#     current_day = (today - start_date).days + 1
#
#     results = []
#     keys = await redis_client.keys("result:*")
#     for key in keys:
#         task_id = key.decode().split(":")[-1]
#         raw = await redis_client.get(key)
#         if not raw:
#             continue
#         try:
#             result = json.loads(raw)
#         except:
#             result = raw.decode()
#
#         results.append({
#             "task_id": task_id,
#             "instruction": f"[task {task_id}]",
#             "result": result
#         })
#
#     return {"cdp_url": cdp_url, "current_day": current_day, "results": results}
#
# @app.get("/failed_tasks/")
# async def get_failed_tasks(cdp_url: str):
#     keys = await redis_client.keys("failed:*")
#     result = []
#     for key in keys:
#         raw = await redis_client.get(key)
#         if not raw:
#             continue
#         data = json.loads(raw)
#         if data["task"]["cdp_url"] == cdp_url:
#             result.append({
#                 "instruction": data["task"]["instruction"],
#                 "error": data["error"]
#             })
#     return result
#
# @app.get("/pending_tasks/")
# async def get_pending_tasks(cdp_url: str):
#     pattern = f"{QUEUE_NAME}:{cdp_url}:*"
#     keys = await redis_client.keys(pattern)
#     all_tasks = []
#
#     for key in keys:
#         day = key.decode().split(":")[-1]
#         items = await redis_client.lrange(key, 0, -1)
#         for raw in items:
#             try:
#                 task = json.loads(raw)
#                 task["day"] = int(day)
#                 all_tasks.append(task)
#             except:
#                 continue
#
#     return all_tasks
#
# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(start_workers())

import asyncio
import json
import logging
import os
from uuid import uuid4
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from datetime import datetime
import redis.asyncio as redis
import aiohttp
import aiofiles
from browser_use import Agent, Controller
from browser_use.browser import BrowserSession
from langchain_ollama import ChatOllama

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
redis_client = redis.Redis()

QUEUE_NAME = "BROWSER_TASK_QUEUE"
START_DATE_KEY = "cdp_start_date"
TASKS_DIR = "./tasks"  # Папка с .txt файлами
PROXY_CHANGE_DELAY = 60  # Константа для задержки после смены прокси
BROWSER_VISION_HOST = os.getenv("BROWSER_VISION_HOST", "localhost")  # Хост для CDP URL

# Словарь для хранения блокировок по очередям
queue_locks = {}


def get_queue_lock(profile_id: str, day: int) -> asyncio.Lock:
    """Возвращает или создаёт блокировку для конкретной очереди (profile_id:day)"""
    queue_key = f"{profile_id}:{day}"
    if queue_key not in queue_locks:
        queue_locks[queue_key] = asyncio.Lock()
    return queue_locks[queue_key]


async def change_proxy(proxy_change_url: str):
    """Вызывает URL для смены прокси"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(proxy_change_url) as response:
                if response.status != 200:
                    logger.error(f"Failed to change proxy: {response.status}")
                    raise HTTPException(status_code=500, detail="Failed to change proxy")
                logger.info(f"Proxy changed via {proxy_change_url}")
        except Exception as e:
            logger.error(f"Error changing proxy: {e}")
            raise


async def start_browser_profile(profile_id: str, folder_id: str, x_token: str) -> str:
    """Запускает профиль через API Browser Vision и возвращает CDP URL"""
    url = f"http://127.0.0.1:3030/start/{folder_id}/{profile_id}"
    headers = {"X-Token": x_token,
               'Content-Type': 'application/json'}
    data = {"args": ["--start-maximized"]}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, json=data) as response:
                if response.status != 200:
                    logger.error(f"Failed to start profile {profile_id}: {response.status}")
                    raise HTTPException(status_code=response.status, detail="Failed to start profile")
                result = await response.json()
                if "port" not in result:
                    logger.error(f"Invalid response for profile {profile_id}: {result}")
                    raise HTTPException(status_code=500, detail="No port in response")
                port = result["port"]
                cdp_url = f"http://{BROWSER_VISION_HOST}:{port}"
                logger.info(f"Profile {profile_id} started, CDP URL: {cdp_url}")
                return cdp_url
        except Exception as e:
            logger.error(f"Error starting profile {profile_id}: {e}")
            raise


async def stop_browser_profile(profile_id: str, folder_id: str,x_token: str):
    """Останавливает профиль через API Browser Vision"""
    url = f"http://127.0.0.1:3030/stop/{folder_id}/{profile_id}"
    headers = {"X-Token": x_token}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    logger.error(f"Failed to stop profile {profile_id}: {response.status}")
                    raise HTTPException(status_code=response.status, detail="Failed to stop profile")
                result = await response
                if result != f"Stopping profile {profile_id} in folder {folder_id}":
                    logger.error(f"Failed to stop profile {profile_id}: {result}")
                    raise HTTPException(status_code=500, detail="Profile stop failed")
                logger.info(f"Profile {profile_id} stopped")
        except Exception as e:
            logger.error(f"Error stopping profile {profile_id}: {e}")
            raise


def parse_txt_to_tasks(file_content: str, file_name: str = "uploaded_file") -> list:
    """Парсит содержимое .txt файла с задачами"""
    tasks = []
    try:
        lines = file_content.strip().split("\n")
        profile_id = None
        folder_id = None
        proxy_change_url = None  # Опциональное поле
        x_token = None
        current_day = None

        for line in lines:
            line = line.strip()
            if line.startswith("profile_id:"):
                profile_id = line.split(":", 1)[1].strip()
            elif line.startswith("folder_id:"):
                folder_id = line.split(":", 1)[1].strip()
            elif line.startswith("proxy_change_url:"):
                proxy_change_url = line.split(":", 1)[1].strip()
            elif line.startswith("x_token:"):
                x_token = line.split(":", 1)[1].strip()
            elif line.isdigit():
                current_day = int(line)
            elif line and current_day is not None:
                for instruction in line.split(";"):
                    task = {
                        "task_id": str(uuid4()),
                        "day": current_day,
                        "instruction": instruction.strip(),
                        "profile_id": profile_id,
                        "folder_id": folder_id,
                        "proxy_change_url": proxy_change_url,  # Может быть None
                        "x_token": x_token,
                        "retries_left": 3
                    }
                    tasks.append(task)
                    logger.debug(f"Parsed task: {task}")
        # Проверка обязательных полей
        required_fields = ["profile_id", "folder_id", "x_token"]
        for field in required_fields:
            if not locals().get(field):
                raise ValueError(f"Missing required field: {field} in {file_name}")
    except Exception as e:
        logger.error(f"Error parsing {file_name}: {e}")
        raise
    logger.info(f"Parsed {len(tasks)} tasks from {file_name}")
    return tasks


async def scan_tasks_directory():
    """Сканирует папку с .txt файлами и добавляет задачи в очередь"""
    if not os.path.exists(TASKS_DIR):
        logger.error(f"Tasks directory {TASKS_DIR} does not exist")
        return

    for filename in os.listdir(TASKS_DIR):
        if filename.endswith(".txt"):
            file_path = os.path.join(TASKS_DIR, filename)
            try:
                async with aiofiles.open(file_path, mode="r", encoding="utf-8") as file:
                    content = await file.read()
                tasks = parse_txt_to_tasks(content, filename)
                today = datetime.today().date()
                for task in tasks:
                    queue_name = f"{QUEUE_NAME}:{task['profile_id']}:{task['day']}"
                    await redis_client.set(f"{START_DATE_KEY}:{task['profile_id']}", today.isoformat())
                    await redis_client.rpush(queue_name, json.dumps(task))
                    logger.info(f"Queued task {task['task_id']} for profile {task['profile_id']}, day {task['day']}")
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")


async def run_browser_task(task):
    """Выполняет задачу с использованием browser-use и Browser Vision API"""

    user_task = task["instruction"]
    profile_id = task["profile_id"]
    folder_id = task["folder_id"]
    proxy_change_url = task["proxy_change_url"]
    x_token = task["x_token"]

    # Смена прокси, если указан proxy_change_url
    if proxy_change_url:
        await change_proxy(proxy_change_url)
        await asyncio.sleep(PROXY_CHANGE_DELAY)
        logger.info(f"Waited {PROXY_CHANGE_DELAY} seconds after proxy change")

    # Запуск профиля через API Browser Vision
    cdp_url = await start_browser_profile(profile_id, folder_id, x_token)

    logger.info(f"Запуск задачи {task['task_id']} через CDP: {cdp_url}")

    browser_session = BrowserSession(headless=False, cdp_url=cdp_url)
    controller = Controller()
    controller.action(description="go to about:blank")
    agent = Agent(
        task=user_task,
        llm=ChatOllama(model="qwen2.5:32b", num_ctx=32000),
        tool_calling_method='raw',
        controller=controller,
        browser_session=browser_session,
    )

    try:
        result = await agent.run()
    finally:
        await browser_session.close()
        logger.info(f"Сессия закрыта для {cdp_url}")
        await stop_browser_profile(profile_id, folder_id, x_token)

    try:
        result_str = json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        result_str = f"Failed to serialize result: {str(e)}"

    await redis_client.set(f"result:{task['profile_id']}:{task['task_id']}", result_str)
    logger.info(f"Результат сохранён: task_id={task['task_id']}")


async def handle_task(task, semaphore):
    """Обрабатывает задачу с блокировкой для последовательного выполнения"""
    queue_lock = get_queue_lock(task['profile_id'], task['day'])
    async with semaphore:
        async with queue_lock:
            try:
                async with asyncio.timeout(300):
                    await run_browser_task(task)
            except Exception as e:
                task['retries_left'] -= 1
                if task['retries_left'] > 0:
                    queue_name = f"{QUEUE_NAME}:{task['profile_id']}:{task['day']}"
                    await redis_client.rpush(queue_name, json.dumps(task))
                    logger.warning(
                        f"Retrying task {task['task_id']} (profile_id: {task['profile_id']}, "
                        f"day: {task['day']}, instruction: {task['instruction'][:50]}...) "
                        f"— retries left: {task['retries_left']}"
                    )
                else:
                    await redis_client.set(f"failed:{task['task_id']}", json.dumps({
                        "task": task,
                        "error": str(e),
                        "failed_at": datetime.now().isoformat()
                    }))
                    logger.error(
                        f"Task {task['task_id']} (profile_id: {task['profile_id']}, day: {task['day']}) "
                        f"failed permanently: {e}"
                    )


async def worker_for_profile(profile_id: str, semaphore):
    """Воркер для обработки задач конкретного profile_id"""
    while True:
        start_date_str = await redis_client.get(f"{START_DATE_KEY}:{profile_id}")
        if not start_date_str:
            await asyncio.sleep(10)
            continue

        try:
            start_date = datetime.fromisoformat(start_date_str.decode()).date()
        except:
            await asyncio.sleep(10)
            continue

        today = datetime.today().date()
        day_num = (today - start_date).days + 1
        queue_name = f"{QUEUE_NAME}:{profile_id}:{day_num}"

        task_json = await redis_client.lpop(queue_name)
        if not task_json:
            await asyncio.sleep(5)
            continue

        task = json.loads(task_json)
        await handle_task(task, semaphore)


async def discover_profile_ids():
    """Находит все profile_id из Redis"""
    keys = await redis_client.keys(f"{START_DATE_KEY}:*")
    return [key.decode().split(":", 1)[1] for key in keys]


async def start_workers(max_concurrent_tasks: int = 10):
    """Запускает воркеры для всех profile_id"""
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    while True:
        profile_ids = await discover_profile_ids()
        for profile_id in profile_ids:
            asyncio.create_task(worker_for_profile(profile_id, semaphore))
        await asyncio.sleep(60)


@app.post("/upload_txt/")
async def upload_txt(file: UploadFile = File(...)):
    """Эндпоинт для загрузки .txt файла с задачами"""
    if not file.filename.endswith(".txt"):
        raise HTTPException(status_code=400, detail="File must be a .txt file")

    try:
        content = (await file.read()).decode("utf-8")
        tasks = parse_txt_to_tasks(content, file.filename)
        today = datetime.today().date()
        for task in tasks:
            queue_name = f"{QUEUE_NAME}:{task['profile_id']}:{task['day']}"
            await redis_client.set(f"{START_DATE_KEY}:{task['profile_id']}", today.isoformat())
            await redis_client.rpush(queue_name, json.dumps(task))
            logger.info(f"Queued task {task['task_id']} for profile {task['profile_id']}, day {task['day']}")
        return JSONResponse({"status": "ok", "tasks_queued": len(tasks)})
    except Exception as e:
        logger.error(f"Error processing uploaded file {file.filename}: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to process file: {str(e)}")


@app.get("/user_status/")
async def get_user_status(profile_id: str):
    start_date_str = await redis_client.get(f"{START_DATE_KEY}:{profile_id}")
    if not start_date_str:
        raise HTTPException(status_code=404, detail="Profile ID не найден")

    start_date = datetime.fromisoformat(start_date_str.decode()).date()
    today = datetime.today().date()
    current_day = (today - start_date).days + 1

    keys = await redis_client.keys(f"result:{profile_id}:*")
    results = []

    for key in keys:
        task_id = key.decode().split(":")[-1]
        raw = await redis_client.get(key)
        if not raw:
            continue
        try:
            result = json.loads(raw)
        except:
            result = raw.decode()

        results.append({
            "task_id": task_id,
            "instruction": f"[task {task_id}]",
            "result": result
        })

    return {"profile_id": profile_id, "current_day": current_day, "results": results}


@app.get("/failed_tasks/")
async def get_failed_tasks(profile_id: str):
    keys = await redis_client.keys("failed:*")
    result = []
    for key in keys:
        raw = await redis_client.get(key)
        if not raw:
            continue
        data = json.loads(raw)
        if data["task"]["profile_id"] == profile_id:
            result.append({
                "instruction": data["task"]["instruction"],
                "error": data["error"]
            })
    return result


@app.get("/pending_tasks/")
async def get_pending_tasks(profile_id: str):
    pattern = f"{QUEUE_NAME}:{profile_id}:*"
    keys = await redis_client.keys(pattern)
    all_tasks = []

    for key in keys:
        day = key.decode().split(":")[-1]
        items = await redis_client.lrange(key, 0, -1)
        for raw in items:
            try:
                task = json.loads(raw)
                task["day"] = int(day)
                all_tasks.append(task)
            except:
                continue

    return all_tasks

@app.get("/start_scan_and_tasks")
async def start_scan():
    asyncio.create_task(scan_tasks_directory())
    return f"App successfully started! Initializing scan of {TASKS_DIR} directory and queuing tasks!"


@app.on_event("startup")
async def startup_event():
    """Запускает сканирование папки и воркеры при старте"""
    asyncio.create_task(start_workers())