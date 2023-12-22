from huiAudioCorpus.dependencyInjection.DependencyInjection import DependencyInjection
import time
import os
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--database_path", type=str, default=None, help="Set directory where overview file should be created.")
    parser.add_argument("-l", "--language", type=str, default="English", help="Set the language of the books that should be retrieved.")
    parser.add_argument("--request_url", type=str, default=None, help="Set custom request URL for metadata retrieval.")
    args = parser.parse_args()

    if args.database_path:
        database_path = args.database_path
    else:
        repo_root_path = os.path.join(os.path.dirname(__file__), "..")
        database_dir = "database"
        database_path = os.path.join(repo_root_path, database_dir)
    request_url = args.request_url

    os.makedirs(database_path, exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    step0_path = os.path.join(database_path, f"overview_{timestamp}")
    config = {
        'audios_from_librivox_persistenz': {
            'book_name': '',
            'save_path': '',
            'chapter_path': ''
        },
        'step0_overview': {
            'save_path': step0_path,
            "request_url": request_url,
            "language": args.language.capitalize()
        }
    }
    DependencyInjection(config).step0_overview.run()
