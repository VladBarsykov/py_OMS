import requests
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import os
import chardet
import xml.etree.ElementTree as ET
from pathlib import Path

# Получаем путь к директории, где находится исполняемый файл
current_dir = os.path.dirname(os.path.abspath(__file__))

new_directory = current_dir  # Замените на нужный путь

try:
    os.chdir(new_directory)  # Изменяем рабочий каталог
    print(f"Текущий рабочий каталог изменён на: {os.getcwd()}")
except FileNotFoundError:
    print(f"Ошибка: Директория '{new_directory}' не найдена.")
except PermissionError:
    print(f"Ошибка: У вас нет прав доступа к директории '{new_directory}'.")
except Exception as e:
    print(f"Произошла ошибка: {e}")

print("Текущий рабочий каталог:", os.getcwd())

zip_file_path = 'downloaded_file.zip'  # Путь к ZIP-архиву
file_name_in_zip = 'TAR_XML/VISITTAR.xml'  # Имя файла внутри архива
extract_directory = 'export'  # Директория для извлечения
page_url = 'http://www.omsmurm.ru/Home/Page?menuItem=148&pageId=134'  # Замените на реальный URL страницы
search_phrase = 'TAR_XML.zip'  # Словосочетание для поиска
filename = 'folder_visittar_for_comparison/VISITTAR.XML'  # Задаем имя файла
local_file_path = os.path.join(current_dir, filename)  # Создаем полный путь к файлу
local_file_path = os.path.join(local_file_path) # Путь к локальному файлу
print(local_file_path)
output_file = 'result.txt'  # Имя файла для записи результатов

file_to_compare = os.path.join(extract_directory, file_name_in_zip)
print(f"Путь к файлу для сравнения: {file_to_compare}")
print(file_to_compare)

try:
    # Создаем директорию для извлечения, если она не существует
    os.makedirs(extract_directory, exist_ok=True)
    print(f"Директория '{extract_directory}' успешно создана или уже существует.")
except Exception as e:
    print(f"Произошла ошибка при создании директории: {e}")

def extract_dynamic_link(page_url, search_phrase):
    try:
        # Загрузка HTML-страницы
        response = requests.get(page_url)
        response.raise_for_status()  # Проверка на ошибки

        # Установка кодировки
        response.encoding = response.apparent_encoding

        # Парсинг HTML с помощью BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Поиск всех ссылок на странице
        links = soup.find_all('a')

        # Фильтрация ссылок по наличию заданной фразы в конце
        for link in links:
            href = link.get('href')
            if href and href.endswith(search_phrase):
                return href

        raise ValueError("Не удалось найти динамическую ссылку с заданным словосочетанием.")
    
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке страницы: {e}")
    except Exception as e:
        print(f"Произошла ошибка при извлечении ссылки: {e}")

dynamic_link = extract_dynamic_link(page_url, search_phrase)

# Функция для разархивирования zip файла после скачивания
def extract_file_from_zip(zip_path, file_name, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Проверяем, существует ли файл в архиве
            if file_name in zip_ref.namelist():
                zip_ref.extract(file_name, extract_to)
                print(f"Файл '{file_name}' успешно извлечен в '{extract_to}'")
            else:
                print(f"Файл '{file_name}' не найден в архиве.")
    except zipfile.BadZipFile:
        print(f"Ошибка: '{zip_path}' не является корректным ZIP-файлом.")
    except FileNotFoundError:
        print(f"Ошибка: Файл '{zip_path}' не найден.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")

def download_file(url, local_filename):
    try:
        with requests.get(url) as response:
            response.raise_for_status()  # Проверка на ошибки
            with open(local_filename, 'wb') as f:
                f.write(response.content)
        return local_filename
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке файла: {e}")
        return None

if dynamic_link:
    print(f"Найдена динамическая ссылка: {dynamic_link}")

    # Если ссылка относительная, сделаем её абсолютной
    if not dynamic_link.startswith('http'):
        dynamic_link = requests.compat.urljoin(page_url, dynamic_link)

    # Скачиваем файл по динамической ссылке
    downloaded_file = download_file(dynamic_link, 'downloaded_file.zip')

    # Извлекаем файл из ZIP после успешной загрузки
    if downloaded_file:
        extract_file_from_zip(downloaded_file, file_name_in_zip, extract_directory)
# Параметры

dynamic_link = extract_dynamic_link(page_url, search_phrase)

# Извлекаем файл
extract_file_from_zip(zip_file_path, file_name_in_zip, extract_directory)

# Указываем путь к извлеченному файлу


# Для фикса ошибок с кодировкой создаем функцию в которой задаем кодировку
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read(100000)  # Читаем первые 100000 байт для анализа
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        print(f"Определенная кодировка: {encoding} (доверие: {confidence})")
        return encoding
    
def read_xml_to_dataframe(file_path, tags):
    try:
        print(f"Расположение файла - {file_path}")
        tree = ET.parse(file_path)
        root = tree.getroot()

        data = []
        for item in root.findall('.//*'):
            record = {}
            for tag in tags:
                # Ищем только указанные теги
                element = item.find(tag)
                record[tag] = element.text if element is not None else None
            data.append(record)

        return pd.DataFrame(data)
    except Exception as e:
        print(f"Ошибка при чтении XML-файла: {e}")
        return None

def compare_xml_files_and_output_differences(file1, file2, output_file, tags):
    try:
        df1 = read_xml_to_dataframe(file1, tags)
        df2 = read_xml_to_dataframe(file2, tags)

        if df1 is None or df2 is None:
            print("Не удалось загрузить один из файлов для сравнения.")
            return

        # Проверка наличия всех требуемых тегов в DataFrame
        missing_tags_df1 = [tag for tag in tags if tag not in df1.columns]
        missing_tags_df2 = [tag for tag in tags if tag not in df2.columns]

        if missing_tags_df1:
            print(f"В первом файле отсутствуют следующие теги: {missing_tags_df1}")
        if missing_tags_df2:
            print(f"Во втором файле отсутствуют следующие теги: {missing_tags_df2}")

        # Сброс индексов
        df1.reset_index(drop=True, inplace=True)
        df2.reset_index(drop=True, inplace=True)

        # Проверка на дубликаты
        if df1.duplicated().any():
            print("В первом DataFrame есть дубликаты. Удаляем дубликаты.")
            df1 = df1.drop_duplicates()
        if df2.duplicated().any():
            print("Во втором DataFrame есть дубликаты. Удаляем дубликаты.")
            df2 = df2.drop_duplicates()

        # Проверка на количество строк
        if len(df1) != len(df2):
            print(f"Количество строк в DataFrame не совпадает: {len(df1)} и {len(df2)}.")
            # Находим различия между файлами
            diff_df1 = df1.merge(df2, how='outer', indicator=True)
            only_in_df1 = diff_df1[diff_df1['_merge'] == 'left_only']
            only_in_df2 = diff_df1[diff_df1['_merge'] == 'right_only']

            with open(output_file, 'w', encoding='utf-8') as out:
                out.write("Различия между файлами:\n\n")
                
                # out.write("Строки только в первом файле:\n")
                #out.write(only_in_df1.to_string(index=False))
                #out.write("\n\n")

                #out.write("Строки только во втором файле:\n")
                out.write(only_in_df2.to_string(index=False))
                
            print(f"Различия записаны в файл: {output_file}")
            return

        # Убедимся, что столбцы идентичны по имени и порядку
        if not df1.columns.equals(df2.columns):
            print("Столбцы DataFrame не совпадают. Сравнение невозможно.")
            print(f"Столбцы первого файла: {df1.columns.tolist()}")
            print(f"Столбцы второго файла: {df2.columns.tolist()}")
            return

        # Проверка типов данных
        if not all(df1.dtypes == df2.dtypes):
            print("Типы данных столбцов не совпадают.")
            print(f"Типы первого файла: {df1.dtypes}")
            print(f"Типы второго файла: {df2.dtypes}")
            return

        # Сравниваем DataFrame
        comparison_result = df1.compare(df2)

        with open(output_file, 'w', encoding='utf-8') as out:
            if comparison_result.empty:
                out.write("Файлы идентичны.\n")
            else:
                out.write("Различия:\n")
                out.write(comparison_result.to_string())

        print(f"Различия записаны в файл: {output_file}")

    except Exception as e:
        print(f"Произошла ошибка при сравнении файлов: {e}")


tags_to_compare = ['id_r_visit', 'id_tarif', 'code', 'prvs', 'sub_hosp', 'purpose', 'vid_fin',
                   'tarif', 'tarif_1', 'tarif_3', 'tarif_4', 'tarif_6', 'd_from', 'd_to',
                   'comment']
        # Теперь сравниваем файлы
compare_xml_files_and_output_differences(local_file_path, file_to_compare, output_file, tags_to_compare)
