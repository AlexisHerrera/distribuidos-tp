import yaml
import csv
import os


class IndentDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def read_uuids_from_csv(csv_path: str) -> list[str]:
    try:
        with open(csv_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            row = next(reader)
            if not row:
                raise ValueError("CSV file is empty or improperly formatted.")
            return row
    except FileNotFoundError:
        print(f"UUID CSV file not found at: {csv_path}")
        raise
    except Exception as e:
        print(f"Error reading UUID CSV file: {e}")
        raise


def create_config(clients_uuids: list[str]) -> dict:
    config = {
        'rabbit': {
            'host': 'rabbitmq'
        },
        'connection': {
            'consumer': [
                {
                    'type': 'direct',
                    'queue': 'reporter'
                }
            ],
            'publisher': [
                {
                    'type': 'broadcast',
                    'exchange': 'movies_metadata_clean',
                    'msg_type': 'Movie'
                }
            ]
        },
        'heartbeat': {
            'port': 13434
        },
        'log': {
            'level': 'INFO'
        }
    }

    for client_id in clients_uuids:
        config['connection']['publisher'].append({
            'type': 'direct',
            'queue': f'ratings_clean_{client_id}',
            'msg_type': 'Rating'
        })
        config['connection']['publisher'].append({
            'type': 'direct',
            'queue': f'credits_clean_{client_id}',
            'msg_type': 'Cast'
        })

    return config


def create_config_file_cleaner(clients_uuids: list[str], output_config_path: str):
    config = create_config(clients_uuids)

    os.makedirs(os.path.dirname(output_config_path), exist_ok=True)

    with open(output_config_path, 'w') as file:
        yaml.dump(config, file, Dumper=IndentDumper, default_flow_style=False, sort_keys=False)

    print(f"Cleaner configuration file '{output_config_path}' created successfully with {len(clients_uuids)} UUID-based queues.")


def create_config_files_ratings_joiner(clients_uuids: list[str], output_directory: str):
    os.makedirs(output_directory, exist_ok=True)

    for client_id in clients_uuids:
        joiner_config = {
            'rabbit': {
                'host': 'rabbitmq'
            },
            'connection': {
                'consumer': [
                    {
                        'type': 'direct',
                        'queue': f'ratings_clean_{client_id}'
                    }
                ],
                'publisher': [
                    {
                        'type': 'direct',
                        'queue': 'movies_rating_joined'
                    }
                ],
                'base_db': [
                    {
                        'type': 'broadcast',
                        'queue': 'argentina_post_2000_ratings',
                        'exchange': 'argentina_post_2000'
                    }
                ]
            },
            'heartbeat': {
                'port': 13434
            },
            'log': {
                'level': 'INFO'
            }
        }

        joiner_config_filename = os.path.join(output_directory, f'ratings_joiner_config_{client_id}.yaml')

        with open(joiner_config_filename, 'w') as file:
            yaml.dump(joiner_config, file, Dumper=IndentDumper, default_flow_style=False, sort_keys=False)

        print(f"Ratings joiner configuration file created: {joiner_config_filename}")


def create_config_files_cast_joiner(clients_uuids: list[str], output_directory: str):
    os.makedirs(output_directory, exist_ok=True)

    for client_id in clients_uuids:
        cast_config = {
            'rabbit': {
                'host': 'rabbitmq'
            },
            'connection': {
                'consumer': [
                    {
                        'type': 'direct',
                        'queue': f'credits_clean_{client_id}'
                    }
                ],
                'publisher': [
                    {
                        'type': 'direct',
                        'queue': 'movies_cast_joined'
                    }
                ],
                'base_db': [
                    {
                        'type': 'broadcast',
                        'queue': 'argentina_post_2000_cast',
                        'exchange': 'argentina_post_2000'
                    }
                ]
            },
            'heartbeat': {
                'port': 13434
            },
            'log': {
                'level': 'INFO'
            }
        }

        cast_config_filename = os.path.join(output_directory, f'cast_joiner_config_{client_id}.yaml')

        with open(cast_config_filename, 'w') as file:
            yaml.dump(cast_config, file, Dumper=IndentDumper, default_flow_style=False, sort_keys=False)

        print(f"Cast joiner configuration file created: {cast_config_filename}")


def main():
    uuid_csv_path = 'clients_uuids.csv'
    cleaner_config_path = 'src/server/cleaner/config.yaml'
    joiners_dir = 'src/server/joiners/'

    try:
        clients_uuids = read_uuids_from_csv(uuid_csv_path)

        create_config_file_cleaner(clients_uuids, cleaner_config_path)
        create_config_files_ratings_joiner(clients_uuids, joiners_dir)
        create_config_files_cast_joiner(clients_uuids, joiners_dir)

    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()
