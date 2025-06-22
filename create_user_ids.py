import uuid
import yaml
import csv


def generate_user_id() -> uuid.UUID:
    return uuid.uuid4()


def read_num_clients(scaling_config_path: str) -> int:
    try:
        with open(scaling_config_path, 'r') as f:
            data = yaml.safe_load(f)
            num_clients = data.get('clients', None)
            if not isinstance(num_clients, int) or num_clients < 1:
                raise ValueError("Invalid or missing 'clients' key in scaling config.")
            return num_clients
    except FileNotFoundError:
        print(f"Scaling config file not found at: {scaling_config_path}")
        raise
    except yaml.YAMLError as e:
        print(f"Error parsing YAML: {e}")
        raise


def generate_uuids_csv(num_clients: int, output_path: str):
    uuids = [str(generate_user_id()) for _ in range(num_clients)]

    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(uuids)

    print(f"UUIDs CSV file created successfully at '{output_path}' with {num_clients} entries.")


def main():
    scaling_config_path = 'nodes-config.yaml'
    output_csv_path = 'clients_uuids.csv'

    try:
        num_clients = read_num_clients(scaling_config_path)
        generate_uuids_csv(num_clients, output_csv_path)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()
