from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import json
import os

class cleanData:
    def __init__(self, decode_path:str):
        self.decode_file = json.load(open(decode_path))
        self.data_dir = 'data/raw'

    def make_dataset(self):

        cleaned_df = pd.DataFrame(columns=self.decode_file.keys())
        for folder in os.listdir(self.data_dir):
            count = 0
            if folder == '.gitkeep':
                continue
            else:
                for file in os.listdir('data/raw/' + folder):
                    if os.path.exists('data/processed/' + file + '.csv',):
                        continue
                    else:
                        df = self.clean_txt('data/raw/' + folder + '/' + file)
                        cleaned_df = pd.concat([cleaned_df, df])
                        print('Processed '+ folder + '_' + str(count))
                        count += 1
        return cleaned_df

    def process_line(self, line):
        temp_df = []
        for key in self.decode_file:
            data = line[self.decode_file[key]['position']-1: self.decode_file[key]['position'] + self.decode_file[key]['length']-1]
            try:
                data = data.decode('utf-8')
            except UnicodeDecodeError:
                pass
            data = data.strip()
            temp_df.append(data)
        return temp_df

    def clean_txt(self, dev_file):
        df = pd.DataFrame(columns=self.decode_file.keys())
        with ThreadPoolExecutor() as executor:
            lines = [line for line in open(dev_file, 'rb')]
            results = list(executor.map(self.process_line, lines))
        df = pd.concat([df, pd.DataFrame(results, columns=self.decode_file.keys())])
        return df
