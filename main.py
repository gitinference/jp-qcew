from src.visualization.graph import graphGenerator


def main():
    # cleanData().make_qcew_dataset()
    # print(cleanData().group_by_naics_code().to_pandas())
    # print(cleanData().unique_naics_code().to_pandas())
    graphGenerator().create_graph("5412")


if __name__ == "__main__":
    main()
