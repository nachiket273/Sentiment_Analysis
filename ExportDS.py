import os
import pandas as pd
import sys
sys.path.insert(1, './Config')

# Refrence: https://gist.github.com/wpm/52758adbf506fd84cff3cdc7fc109aad

from Config import get_config


configs = get_config()

DATASET_FOLDER = configs['dataset_dir'][1:-1]
SENTENCES_FILE = os.path.join(DATASET_FOLDER, 'datasetSentences.txt')
LABEL_FILE = os.path.join(DATASET_FOLDER, 'sentiment_labels.txt')
DICT_FILE = os.path.join(DATASET_FOLDER, 'dictionary.txt')
SPLIT_FILE = os.path.join(DATASET_FOLDER, 'datasetSplit.txt')
DATA_DIR = configs['data_dir'][1:-1]


def get_split_name(split_num):
    name = {1: "train", 2: "test", 3: "dev"}
    return name[split_num]


def export():
    phrases_df = pd.read_csv(DICT_FILE, sep='|')
    phrases_df.columns = ["phrase", "id"]
    phrases_df = phrases_df.set_index("id")

    score_df = pd.read_csv(LABEL_FILE, sep='|')
    score_df.columns = ["id", "score"]
    score_df = score_df.set_index("id")
    phrases_df = phrases_df.join(score_df)

    sentence_df = pd.read_csv(SENTENCES_FILE, index_col="sentence_index",
                              sep='\t')
    split_df = pd.read_csv(SPLIT_FILE, index_col="sentence_index")
    sentence_df = sentence_df.join(split_df).set_index('sentence')

    data_df = phrases_df.join(sentence_df, on="phrase")
    data_df["splitset_label"] = data_df["splitset_label"].fillna(1).astype(int)
    data_df["phrase"] = data_df["phrase"].str\
        .replace(r"\s('s|'d|'re|'ll|'m|'ve|n't)\b", lambda m: m.group(1))

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)

    for split, data in data_df.groupby("splitset_label"):
        csv_name = get_split_name(split) + ".csv"
        csv_path = os.path.join(DATA_DIR, csv_name)
        data.to_csv(csv_path)


if __name__ == "__main__":
    export()
