from jp_qcew import CleanQCEW


def main():
    return CleanQCEW(saving_dir="data/").make_qcew_dataset()


if __name__ == "__main__":
    print(main())
