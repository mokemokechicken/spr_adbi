from datetime import datetime

from spr_adbi.client.adbi_client import create_client


def main():
    client = create_client()
    job = client.request('test.echo', ["hello", str(datetime.now())])
    is_success = job.wait()
    if not is_success:
        print("finish error")
    else:
        print("finish success")
        output = job.get_output()
        filenames = output.get_filenames()
        print(filenames)
        for filename in filenames:
            print(filename)
            print(output.get_file_content(filename))
            print()


if __name__ == '__main__':
    main()
