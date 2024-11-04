from src.data.data_process import cleanData

def main():
    cleanData(decode_path="data/external/decode.json").make_dataset()

if __name__ == "__main__":
    main()
