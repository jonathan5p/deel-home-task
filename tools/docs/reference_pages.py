import mkdocs_gen_files

from pipelines.schemas.reference import reference_pages

for path, text in reference_pages().items():
    with mkdocs_gen_files.open(path, "w") as page:
        page.write(text)
