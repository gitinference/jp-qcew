from src.data.data_process import cleanData

def main():
    cleanData(decode_path="data/external/decode.json").group_by_naics_code()

if __name__ == "__main__":
    main()
