# Season report with Apache Spark

The ledger answers questions about **now** ("how much water does farm-a have left?").
Spark answers questions about the **whole history**, by scanning every record in parallel:
which farms run out of water, who helps whom, how fair the use is, and what share each farm
should get next season.

## Run on the real ledger data
```powershell
python spark/export_ledger.py
docker compose run --rm spark
```
1. Copies the committed records from ledger-1's database into `spark/data/*.csv`.
2. Runs the Spark job (the first run downloads the Spark image, about 1 GB).

Results are printed and also saved as CSV in `spark/output/`.

## Run on a simulated season (more data)
The live demo only creates a few days of records. To show a whole season:
```powershell
python spark/simulate_season.py --days 120
docker compose run --rm spark /app/spark/water_report.py /app/spark/data_simulated /app/spark/output_simulated
```
The simulated data is clearly synthetic: 4 farms, hot and cool days, a 15-day dry spell
with a lower limit, and transfers from farms that need less to farms that need more.

## What the report contains
| Output | Question it answers |
|---|---|
| `farms` | Per farm: average use of its share, days it ran out, water given and received |
| `flows` | Who sends water to whom, and how much |
| `daily` | Per day: reservoir use and fairness (Jain's index, 1.0 = perfectly even) |
| `farm_day` | Every farm on every day |
| `suggestion` | A share for next season, proportional to each farm's real use |
