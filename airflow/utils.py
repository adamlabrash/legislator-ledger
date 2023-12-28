def csv_file_is_initialized(file_name: str) -> bool:
    with open(file_name, 'r') as csvfile:
        return len(csvfile.readlines()) > 0
