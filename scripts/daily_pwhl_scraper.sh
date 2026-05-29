#!/bin/bash
# Scrape raw PWHL game JSON and schedules
# Usage: bash scripts/daily_pwhl_scraper.sh -s 2026 -e 2026 -r TRUE 2>&1 | tee "daily_pwhl.out"

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
    TMPLOG=$(mktemp "/tmp/fastRhockey_pwhl_raw_logfile_${i}.XXXXXX.log")
    echo "=== Processing PWHL season $i ==="
    # Tee inside the block writes to /tmp (untracked) so the `git pull` calls
    # don't trip over their own log output being written to a tracked file.
    {
        git pull >> /dev/null
        git config --local user.email "action@github.com"
        git config --local user.name "Github Action"
        Rscript R/scrape_pwhl_raw.R -s $i -e $i -r $RESCRAPE
        git pull >> /dev/null
        git add pwhl/* >> /dev/null
        git add pwhl/pwhl_schedule_master.* >> /dev/null
        git pull >> /dev/null
        git add . >> /dev/null
        git commit -m "PWHL Raw Updated (Start: $i End: $i)" || echo "No changes to commit"
        git pull >> /dev/null
        git push >> /dev/null
    } 2>&1 | tee "$TMPLOG"

    # Block is finished and pushed; tee has closed $TMPLOG. Now copy the log
    # into its tracked location and commit/push it on its own.
    cp "$TMPLOG" "$LOGFILE"
    git pull --rebase >> /dev/null || true
    git add "$LOGFILE"
    git commit -m "PWHL Raw log update (Start: $i End: $i)" >> /dev/null || echo "No log changes to commit"
    git push >> /dev/null
    rm -f "$TMPLOG"
done
