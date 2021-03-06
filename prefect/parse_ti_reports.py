import os
import re
import uuid

import pdfminer.pdfparser
import pdfplumber
import prefect
from prefect import task, Flow, Client
from prefect.storage import Docker
from prefect.run_configs import KubernetesRun

INPUT_PATH = os.environ.get("INPUT_PATH", "/tireports")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/results")

code_like_regexes = [
    # python
    # some reports put line numbers with their Python. I have no idea why.
    re.compile(r'[\d\s.]*def .+\('),
    re.compile(r'[\d\s]*import .+'),
    re.compile(r'[\d\s]*from .+ import'),
    # C
    re.compile(r'\s*void .+\('),
    re.compile(r'\s*char .+\('),
    re.compile(r'\s*require \'.+\' '),
    re.compile(r'\s*#include'),
    re.compile(r'\s*#define'),
    re.compile(r'\s*#ifndef'),
    re.compile(r'\s*typedef'),
    # java
    re.compile(r'\s*public static '),
    re.compile(r'\s*private static '),
    re.compile(r'\s*public void '),
    re.compile(r'\s*private void '),
    # C#
    re.compile(r'\s*namespace '),
    # assembly
    re.compile(r'\s*push '),
    re.compile(r'\s*mov '),
    re.compile(r'\s*xor '),
    re.compile(r'\s*mul '),
    re.compile(r'\s*inc '),
    re.compile(r'\s*cmp '),
    re.compile(r'\s*jnz '),
    re.compile(r'\s*pop '),
    re.compile(r'\s*ret '),
    # hex dump
    re.compile(r'\s*\d{8} \d{2} \d{2} \d{2}')
]


URL_REGs = [
    # https is optional, handle de-fanging of the colon (for some reason it's popular to put
    # brackets around the colon in the http://, but it's not universal, so make those optional.)
    #
    re.compile(r'http(s?)(\[?):(]?)//.*\s'),
    re.compile(r'hxxp(s?)(\[?):(]?)//.*\s'),
    re.compile(r'/wp-content/.*\s'),
    re.compile(r'\s.*?\.com/.*\s')
    ]



@task
def parse_task(job_id, input_path, output_path):
    logger = prefect.context.get("logger")
    logger.info(f"beginning task with job id {job_id}")
    parse_all(job_id, input_path, output_path)
    logger.info(f"finished task with job id {job_id}")


def parse_all(job_id, input_path, output_path, skip_overwrites=True, logger=None):
    base_dir = os.path.join(output_path, job_id)
    if os.path.exists(base_dir):
        raise Exception(f"output job directory {base_dir} already exists.")
    os.makedirs(base_dir)
    for root, dirs, files in os.walk(input_path):
        for filename in files:
            if filename.endswith(".pdf"):
                path = os.path.join(root, filename)
                text = extract_text_from_PDF(path, logger)
                new_path = os.path.join(base_dir, filename.replace(".pdf", ".txt"))
                if os.path.exists(new_path) and not skip_overwrites:
                    raise Exception(f"overwriting existing data at {new_path}")
                with open(new_path, "w") as outfile:
                    outfile.write(text)


def replace_urls(report_string):
    for reg in URL_REGs:
        if reg.search(report_string):
            report_string = re.sub(reg, " ", report_string)
    return report_string



def print_or_log(error_string, logger):
    if logger is not None:
        logger.error(error_string)
    else:
        print(error_string)


def line_looks_like_code(line: str) -> bool:
    text = line.strip()
    return any(reg.match(text) for reg in code_like_regexes)


def remove_code_snippets(text: str) -> str:
    # assume that a line that starts with a code-like string is the beginning of a code snippet,
    # also assume that there are *two* newlines after a code snippet before returning to regular text.
    in_code: bool = False
    last_line_was_blank = False
    response = ""
    for line in text.split("\n"):
        contents = line.strip()
        if not contents and last_line_was_blank:
            in_code = False
        if line_looks_like_code(line):
            in_code = True
        if not in_code:
            # split removes "\n"'s, so put them back.
            response += line + "\n"
        last_line_was_blank = not bool(contents)
    return response


def extract_text_from_PDF(path_to_pdf, logger) -> str:
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
                        print_or_log(f"bad page {page.page_number} in {path_to_pdf} skipping", logger)
                full_text += text
    except pdfminer.pdfparser.PDFSyntaxError:
        print_or_log(f"bad pdf syntax in {path_to_pdf}. skipping entire file.", logger)
    except Exception as e:
        print_or_log(f"other exception occurred during processing. skipping {path_to_pdf}. Exception: {e}", logger)
    # TODO here: path/mutex/url/ip/domain name munging on extracted text
    full_text = remove_code_snippets(full_text)
    # remove multi-line whitespaces, since they seem to confuse GPT2
    full_text = re.sub(r'\s{2,}', " ", full_text)
    # remove urls, since that seems to also be confusing GPT2
    full_text = replace_urls(full_text)
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
    parse_all("testing", "/Users/g-clef/IdeaProjects/synthetic-ti-reports/prefect/", "/tmp/parsed_results")


if __name__ == "__main__":
    main()
    # test()
