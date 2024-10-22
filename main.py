import os.path
import time
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

from googleapiclient.errors import HttpError

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Если измените этот список, удалите файл token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
# Настройка часовой зоны
MOSCOW_TZ = pytz.timezone('Europe/Moscow')


def authenticate():
    creds = None
    # Файл token.json сохраняет токены пользователя.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    # Если токен недействителен, попросить пользователя авторизоваться.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Сохранение токена в файл.
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds


def update_sheet():
    # Аутентификация
    creds = authenticate()

    # Подключение к Google Sheets API
    service = build('sheets', 'v4', credentials=creds)

    # Данные для обновления
    spreadsheet_id = '147qUPOrVzDGafPq9peu2pAFA5OMPXx09PFw__vFzdsQ'

    # Получаем данные о всех листах в таблице
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get('sheets', [])

    # Найдем индекс нужного листа
    title = 'Четверг'
    range_name = f"{title}!B5:B6"

    # Данные для записи
    values = [
        ['Мартинес Паула'],
        ['Нишанбаев Ильяс']
    ]
    body = {
        'values': values
    }

    # Запись данных в таблицу
    result = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption="RAW", body=body).execute()

    print(f"{result.get('updatedCells')} ячеек обновлено.")


def update_sheet_with_retry():
    while True:
        try:
            # Ваш код для обновления Google Sheet
            print(f"Попытка обновления таблицы в {datetime.now(MOSCOW_TZ)}")
            update_sheet()
            print("Таблица успешно обновлена!")
            break
        except HttpError as error:
            print(f"Ошибка при обновлении таблицы: {error}. Попытка снова через 1 секунду...")
            time.sleep(1)
        except Exception as error:
            print(f"Неизвестная ошибка: {error}. Попытка снова через 1 секунду...")
            time.sleep(1)


# Создание планировщика
scheduler = BlockingScheduler(timezone=MOSCOW_TZ)

# Планируем задачу на каждую среду в 22:00 по московскому времени
scheduler.add_job(update_sheet_with_retry, 'cron', day_of_week='wed', hour=21, minute=59)

try:
    print("Запуск планировщика...")
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    print("Планировщик остановлен.")
