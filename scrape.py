import modal
import usaco_scraper
import generate_probgate_mapping
import probgate_contests_scraper
import extra_test_data_scraper

app = modal.App(
    "usaco-problems",
    image=modal.Image.debian_slim()
    .pip_install("requests", "bs4", "python-dotenv")
    .add_local_file("legacy_file_io_names.json", "/root/legacy_file_io_names.json")
    .add_local_python_source(
        "usaco_scraper",
        "generate_probgate_mapping",
        "probgate_contests_scraper",
        "extra_test_data_scraper",
    ),
    volumes={
        "/root/data_private": modal.Volume.from_name(
            "usaco-problems", create_if_missing=True
        )
    },
)


@app.function(
    secrets=[
        modal.Secret.from_name("probgate"),
        # CSES_USERNAME / CSES_PASSWORD; see extra_test_data_scraper.py
        modal.Secret.from_name("cses"),
    ],
    timeout=60 * 60 * 6,
)
def scrape():
    usaco_scraper.main()
    probgate_contests_scraper.main()
    generate_probgate_mapping.main()
    extra_test_data_scraper.main()
