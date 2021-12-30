import os
import uuid
import pdfplumber
from prefect import task, Flow, Client
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun

INPUT_PATH = os.environ.get("INPUT_PATH", "/tireports")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/results")


@task
def parse_all(job_id):
    for root, dirs, files in os.walk(INPUT_PATH):
        for filename in files:
            if filename.endswith(".pdf"):
                path = os.path.join(root, filename)
                text = extract_text_from_PDF(path)
                new_path = os.path.join(OUTPUT_PATH, job_id, filename)
                if os.path.exists(new_path):
                    raise Exception(f"overwriting existing data at {new_path}")
                with open(new_path, "w") as outfile:
                    outfile.write(text)


def extract_text_from_PDF(path_to_pdf) -> str:
    full_text = ""
    with pdfplumber.open(path_to_pdf) as pdf:
        for page in pdf.pages:
            text = page.extract_text(layout=True)
            full_text += text
    # TODO here: path/mutex/url/ip/domain name munging on extracted text
    return full_text


def main():
    with Flow("parse ti  reports",
              storage=Docker(registry_url="prefect-ui.g-clef.net:5000"),
              # this installs pdfplumber dynamically at every run time. If I were planning
              # on running this multiple times, it would be a better idea to make this a
              # custom docker image.
              run_config=KubernetesRun(env={"EXTRA_PIP_PACKAGES": "pdfplumber"})) as flow:
        parse_all(uuid.uuid4())

    client = Client(api_server="http://prefect-ui.g-clef.net:4200/graphql")

    client.register(flow=flow, project_name="synthetic-ti-reports-project")

    flow.run()


def test():
    result = extract_text_from_PDF("DeadRinger_ Exposing Chinese Threat Actors Targeting Major Telcos.pdf")
    print(result)


if __name__ == "__main__":
    # main()
    test()
