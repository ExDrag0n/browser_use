
import gradio as gr
import aiohttp
import json
import pandas as pd
import logging
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = "http://localhost:8000"  # Базовый URL FastAPI


async def upload_txt_file(file):
    """Отправляет .txt файл на эндпоинт /upload_txt/"""
    if not file.name.endswith(".txt"):
        return "Ошибка: Файл должен иметь расширение .txt"

    async with aiohttp.ClientSession() as session:
        try:
            with open(file.name, "rb") as f:
                form = aiohttp.FormData()
                form.add_field(
                    name="file",
                    value=f,
                    filename=file.name,
                    content_type="text/plain"
                )
                async with session.post(f"{API_BASE_URL}/upload_txt/", data=form) as response:
                    if response.status != 200:
                        logger.error(f"Failed to upload file: {response.status}")
                        return f"Ошибка: Не удалось загрузить файл (статус: {response.status})"
                    result = await response.json()
                    return f"Файл успешно загружен: {result['tasks_queued']} задач добавлено"
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            return f"Ошибка: {str(e)}"


async def create_and_upload_txt(profile_id, folder_id, x_token, proxy_change_url, tasks_df):
    """Создаёт .txt содержимое из формы и отправляет на /upload_txt/"""
    if not all([profile_id, folder_id, x_token]):
        return "Ошибка: Все обязательные поля (profile_id, folder_id, x_token) должны быть заполнены"

    # Формируем содержимое .txt файла
    content = f"profile_id: {profile_id}\n"
    content += f"folder_id: {folder_id}\n"
    if proxy_change_url:
        content += f"proxy_change_url: {proxy_change_url}\n"
    content += f"x_token: {x_token}\n"

    # Обрабатываем DataFrame с задачами
    try:
        tasks_df = pd.DataFrame(tasks_df, columns=["Day", "Tasks"])
        for day, tasks in tasks_df.itertuples(index=False):
            if pd.isna(day) or not tasks:
                continue
            content += f"{int(day)}\n{tasks}\n"
    except Exception as e:
        logger.error(f"Error processing tasks DataFrame: {e}")
        return f"Ошибка: Неверный формат задач ({str(e)})"

    # Отправляем содержимое на /upload_txt/
    async with aiohttp.ClientSession() as session:
        try:
            form = aiohttp.FormData()
            form.add_field(
                name="file",
                value=io.BytesIO(content.encode("utf-8")),
                filename="tasks.txt",
                content_type="text/plain"
            )
            async with session.post(f"{API_BASE_URL}/upload_txt/", data=form) as response:
                if response.status != 200:
                    logger.error(f"Failed to upload generated txt: {response.status}")
                    return f"Ошибка: Не удалось отправить задачи (статус: {response.status})"
                result = await response.json()
                return f"Задачи успешно отправлены: {result['tasks_queued']} задач добавлено"
        except Exception as e:
            logger.error(f"Error uploading generated txt: {e}")
            return f"Ошибка: {str(e)}"


async def get_user_status(profile_id):
    """Получает статус задач для указанного profile_id"""
    if not profile_id:
        return "Ошибка: Укажите profile_id"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_BASE_URL}/user_status/?profile_id={profile_id}") as response:
                if response.status != 200:
                    logger.error(f"Failed to get user status: {response.status}")
                    return f"Ошибка: Не удалось получить статус (статус: {response.status})"
                result = await response.json()
                df = pd.DataFrame(result["results"])
                output = f"Profile ID: {result['profile_id']}\nТекущий день: {result['current_day']}\n\nРезультаты задач:\n"
                if df.empty:
                    output += "Нет выполненных задач"
                else:
                    output += df.to_string(index=False)
                return output
        except Exception as e:
            logger.error(f"Error getting user status: {e}")
            return f"Ошибка: {str(e)}"


async def get_failed_tasks(profile_id):
    """Получает список проваленных задач для указанного profile_id"""
    if not profile_id:
        return "Ошибка: Укажите profile_id"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_BASE_URL}/failed_tasks/?profile_id={profile_id}") as response:
                if response.status != 200:
                    logger.error(f"Failed to get failed tasks: {response.status}")
                    return f"Ошибка: Не удалось получить проваленные задачи (статус: {response.status})"
                result = await response.json()
                df = pd.DataFrame(result)
                output = f"Проваленные задачи для profile_id {profile_id}:\n"
                if df.empty:
                    output += "Нет проваленных задач"
                else:
                    output += df.to_string(index=False)
                return output
        except Exception as e:
            logger.error(f"Error getting failed tasks: {e}")
            return f"Ошибка: {str(e)}"


async def get_pending_tasks(profile_id):
    """Получает список ожидающих задач для указанного profile_id"""
    if not profile_id:
        return "Ошибка: Укажите profile_id"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{API_BASE_URL} / pending_tasks /?profile_id={profile_id}") as response:
                if response.status != 200:
                    logger.error(f"Failed to get pending tasks: {response.status}")
                    return f"Ошибка: Не удалось получить ожидающие задачи (статус: {response.status})"
                result = await response.json()
            df = pd.DataFrame(result)
            output = f"Ожидающие задачи для profile_id {profile_id}:\n"
            if df.empty:
                output += "Нет ожидающих задач"
            else:
                output += df.to_string(index=False)
            return output
        except Exception as e:
            logger.error(f"Error getting pending tasks: {e}")
    return f"Ошибка: {str(e)}"


def create_gradio_interface():
    """Создаёт Gradio интерфейс"""
    with gr.Blocks() as demo:
        gr.Markdown("# Управление задачами Browser Vision")

        with gr.Tab("Создать задачи"):
            profile_id_input = gr.Textbox(label="Profile ID")
            folder_id_input = gr.Textbox(label="Folder ID")
            x_token_input = gr.Textbox(label="X-Token")
            proxy_change_url_input = gr.Textbox(label="Proxy Change URL (опционально)")
            tasks_input = gr.Dataframe(
                headers=["Day", "Tasks"],
                datatype=["number", "str"],
                label="Задачи (введите день и задачи, разделённые ';')",
                value=pd.DataFrame([{"Day": 1, "Tasks": "task1;task2;task3"}])
            )
            create_button = gr.Button("Отправить задачи")
            create_output = gr.Textbox(label="Результат")
            create_button.click(
                fn=create_and_upload_txt,
                inputs=[profile_id_input, folder_id_input, x_token_input, proxy_change_url_input, tasks_input],
                outputs=create_output
            )

        with gr.Tab("Загрузка задач"):
            file_input = gr.File(label="Загрузить .txt файл")
            upload_button = gr.Button("Загрузить")
            upload_output = gr.Textbox(label="Результат загрузки")
            upload_button.click(
                fn=upload_txt_file,
                inputs=file_input,
                outputs=upload_output
            )

        with gr.Tab("Статус задач"):
            profile_id_status = gr.Textbox(label="Profile ID")
            status_button = gr.Button("Получить статус")
            status_output = gr.Textbox(label="Статус")
            failed_button = gr.Button("Получить проваленные задачи")
            failed_output = gr.Textbox(label="Проваленные задачи")
            pending_button = gr.Button("Получить ожидающие задачи")
            pending_output = gr.Textbox(label="Ожидающие задачи")

            status_button.click(
                fn=get_user_status,
                inputs=profile_id_status,
                outputs=status_output
            )
            failed_button.click(
                fn=get_failed_tasks,
                inputs=profile_id_status,
                outputs=failed_output
            )
            pending_button.click(
                fn=get_pending_tasks,
                inputs=profile_id_status,
                outputs=pending_output
            )

    return demo


if __name__ == "__main__":
    demo = create_gradio_interface()
    demo.launch()
