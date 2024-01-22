import sys

sys.path.append("..")
from process_bg_tables.fips_names import generate_state_fips_data


if __name__ == "__main__":
    generate_state_fips_data(save_file=True)