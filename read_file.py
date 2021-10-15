import os


# 顯示指定資料夾的內容
# will get list of file_name

def get_category_file_name(folder_path):
    category_file_list = []
    folder_content = os.listdir(folder_path)

    print(folder_path + '資料夾內容：')
    for item in folder_content:
        if (item.endswith("_category_id.json")):
            category_file_list.append(item)

    for item in category_file_list:
        print(item)

    return category_file_list


def get_video_list(folder_path):
    video_file_list = []
    folder_content = os.listdir(folder_path)

    print(folder_path + '資料夾內容：')
    for item in folder_content:
        if (item.endswith("videos.csv")):
            video_file_list.append(item)
    return video_file_list
