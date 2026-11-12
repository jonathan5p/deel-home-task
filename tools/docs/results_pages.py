from pathlib import Path

import mkdocs_gen_files

from pipelines.reports.results_pages import results_pages

results = Path(mkdocs_gen_files.FilesEditor.current().config["docs_dir"]) / "results"
for path, text in results_pages(results).items():
    with mkdocs_gen_files.open(path, "w") as page:
        page.write(text)
