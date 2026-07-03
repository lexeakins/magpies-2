# Quick Start

Get the app running and process your first 100 records in under 10 minutes.

---

## Prerequisites

- Python 3.10 or higher
- Google Chrome installed (any recent version)
- An Anthropic API key — get one at `console.anthropic.com` (separate from Claude Pro)

---

## 1. Install Dependencies

Open a terminal, navigate to the `magpie_app` folder, and run:

```bash
pip install -r requirements.txt
```

This installs FastAPI, Selenium, ChromeDriver manager, the Anthropic SDK, and everything else the app needs. ChromeDriver downloads automatically on first run — no manual setup.

---

## 2. Add Your Anthropic API Key

Open `magpie_app/.env` and replace the placeholder:

```
ANTHROPIC_API_KEY=sk-ant-your-key-here
```

Save the file. This is the only required configuration before running.

---

## 3. Start the Server

From the `magpie_app` folder:

```bash
python run.py
```

You should see:

```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

Open `http://127.0.0.1:8000` in your browser.

---

## 4. Upload Your File

On the **New Job** page, drag and drop your Excel file onto the upload zone (or click to browse).

Supported files:
- `Klondike.xlsx` — 50-sheet US dataset (~65K rows)
- `Klondike - C.xlsx` — Canada dataset (~14K rows)
- Any other Excel file with company, city, state columns

The app detects multi-sheet files and column positions automatically. No formatting or header rows required.

---

## 5. Review the Cleanse Report

After upload, the app analyses the file and shows:

- How many sheets were found
- How many rows are valid and ready to process
- How many rows were dropped and why (missing company, city, or state)
- Country distribution across the dataset
- A sample of dropped rows so you can spot data issues

Nothing has been processed yet — this is just analysis.

---

## 6. Set a Row Limit for Your First Run

Before clicking **Start**, leave the row limit checked and set to **100**.

This gives you a representative sample to validate the pipeline and review scoring quality before committing to a full dataset run. A 100-row job costs under $0.25 in API calls.

---

## 7. Start the Job

Click **Start Enrichment Job**.

The dashboard switches to the live progress view. You'll see:

- A progress bar tracking completion
- Running counters for: Completed · Found · Retries · Errors
- A rolling table of the last 10 completed records — company name, website URL, confidence score, tier (High / Medium / Low), and status
- Any errors surfaced in a panel below

Each record goes through: Google Maps scrape → website fetch → Haiku validation → confidence score. Expect roughly 15–25 seconds per record depending on Google Maps response time and website load speed.

---

## 8. Download Results

When the job completes, a green **Download Results** banner appears.

Click **Download Results** to save the enriched Excel file. It's also saved automatically to `magpie_app/data/outputs/`.

The output file is color-coded:
- **Green rows** — High confidence (score ≥ 70) — website is likely correct
- **Yellow rows** — Medium confidence (score 40–69) — worth a manual check
- **Red rows** — Low confidence or not found — hit max retries

---

## 9. Tune Settings (Optional)

Click **Settings** in the left sidebar to adjust:

- Confidence thresholds (what counts as High / Low)
- Max retry attempts per record
- Number of parallel scrape and validation workers
- Jina Reader timeout

Changes take effect on the next job. Settings persist for the session.

---

## Running a Full Dataset

Once you're happy with the results on 100 rows:

1. Upload the file again (or use the same upload — the file is cached)
2. **Uncheck** the row limit toggle
3. Click **Start Enrichment Job**

Full Klondike US (~65K rows) at 4 scrape workers will take several hours. You can leave it running — the server handles everything in the background and the progress UI updates in real time. The output file is written when the job completes.

---

## Stopping the Server

Press `Ctrl+C` in the terminal where `run.py` is running.

In-progress jobs are not automatically saved on shutdown. If you need to stop mid-job, note the row count — the output file will contain only completed records up to that point (results are written at job completion, not incrementally). A resume/checkpoint feature is planned for a future version.

---

## Troubleshooting

**Chrome doesn't launch**
- Make sure Google Chrome is installed and up to date.
- ChromeDriver is downloaded automatically by `webdriver-manager`. If it fails, try `pip install --upgrade webdriver-manager`.

**Anthropic API errors**
- Verify your key is set correctly in `.env` with no extra spaces.
- Check your credit balance at `console.anthropic.com`.

**Jina returns empty content**
- Some sites block Jina Reader. Those records will get a `fetch_error` in `haiku_reasoning` and score 0 on Stage 2 signals. They may retry if the overall score is low enough.

**Column detection fails for a sheet**
- The app drops sheets where it can't identify a company + city + state column. The cleanse report will show these as `could_not_detect_columns`. Check that the sheet has at least 3 columns with recognizable content.
