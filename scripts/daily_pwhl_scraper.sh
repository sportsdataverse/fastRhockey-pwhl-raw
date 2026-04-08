#!/bin/bash
# Scrape raw PWHL game JSON and schedules
# Usage: bash scripts/daily_pwhl_scraper.sh -s 2025 -e 2025

while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done

RESCRAPE=${RESCRAPE:-TRUE}
echo "Rescrape set to: $RESCRAPE"
mkdir -p logs
for i in $(seq "${START_YEAR}" "${END_YEAR}")
do
    LOGFILE="logs/fastRhockey_pwhl_raw_logfile_${i}.log"
    echo "=== Processing PWHL season $i ==="
    {
        git pull >> /dev/null
        git config --local user.email "action@github.com"
        git config --local user.name "Github Action"
        Rscript R/scrape_pwhl_raw.R -s $i -e $i -r $RESCRAPE
        git pull >> /dev/null
        git add pwhl/* >> /dev/null
        git add pwhl/pwhl_schedule_master.* >> /dev/null
        git add logs/* >> /dev/null
        git pull >> /dev/null
        git add . >> /dev/null
        git commit -m "PWHL Raw Updated (Start: $i End: $i)" || echo "No changes to commit"
        git pull >> /dev/null
        git push >> /dev/null
    } 2>&1 | tee "$LOGFILE"
done
