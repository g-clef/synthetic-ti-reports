import os
import uuid

import pdfminer.pdfparser
import pdfplumber
from prefect import task, Flow, Client
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun

INPUT_PATH = os.environ.get("INPUT_PATH", "/tireports")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/results")


@task
def parse_task(job_id, input_path, output_path):
    parse_all(job_id, input_path, output_path)


def parse_all(job_id, input_path, output_path, skip_overwrites=True):
    base_dir = os.path.join(output_path, job_id)
    if os.path.exists(base_dir):
        raise Exception(f"output job directory {base_dir} already exists.")
    os.makedirs(base_dir)
    for root, dirs, files in os.walk(input_path):
        for filename in files:
            if filename.endswith(".pdf"):
                path = os.path.join(root, filename)
                text = extract_text_from_PDF(path)
                new_path = os.path.join(base_dir, filename.replace(".pdf", ".txt"))
                if os.path.exists(new_path) and not skip_overwrites:
                    raise Exception(f"overwriting existing data at {new_path}")
                with open(new_path, "w") as outfile:
                    outfile.write(text)


def extract_text_from_PDF(path_to_pdf) -> str:
    full_text = ""
    try:
        with pdfplumber.open(path_to_pdf) as pdf:
            for page in pdf.pages:
                try:
                    text = page.extract_text(layout=True)
                except Exception:
                    # if layout=True exceptions, try without it. If it exceptions again, we've got a
                    # problem.
                    try:
                        text = page.extract_text()
                    except Exception:
                        print(f"bad page {page.page_number} in {path_to_pdf} skipping")
                full_text += text
    except pdfminer.pdfparser.PDFSyntaxError:
        print(f"bad pdf syntax in {path_to_pdf}. skipping entire file.")
    except Exception as e:
        print(f"other exception occurred during processing. skipping {path_to_pdf}. Exception: {e}")
    # TODO here: path/mutex/url/ip/domain name munging on extracted text
    return full_text


def main():
    with Flow("parse ti reports",
              storage=Docker(registry_url="prefect-ui.g-clef.net:5000", dockerfile="Dockerfile"),
              # tried using the "EXTRA_PIP_PACKAGES option here, but it failed to
              # build the container, so I just built it myself.
              run_config=KubernetesRun()) as flow:
        parse_task(str(uuid.uuid4()), INPUT_PATH, OUTPUT_PATH)

    client = Client(api_server="http://prefect-ui.g-clef.net:4200/graphql")

    client.register(flow=flow, project_name="synthetic-ti-reports")


def test():
    parse_all("testing", "/Volumes/ti_reports", "/tmp/parsed_results")


if __name__ == "__main__":
    main()
    # test()
