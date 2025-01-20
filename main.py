from src.data.data_process import cleanData

def main():
    # cleanData().make_qcew_dataset()
    print(cleanData().group_by_naics_code().to_pandas())
    print(cleanData().unique_naics_code().to_pandas())

if __name__ == "__main__":
    main()
