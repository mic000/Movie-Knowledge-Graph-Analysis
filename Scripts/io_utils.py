import os, glob, shutil


def save_csv(df, final_name, sub_folder):
    """
    df: final dataframe export
    final_name: name of the final csv file
    sub_folder: name of the sub folder
    """
    os.makedirs(sub_folder, exist_ok=True)
    temp_path = os.path.join(sub_folder, "temp_" + final_name)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

    part_file = glob.glob(os.path.join(temp_path, "part-*.csv"))[0]
    shutil.copyfile(part_file, os.path.join(sub_folder, final_name))
    shutil.rmtree(temp_path)