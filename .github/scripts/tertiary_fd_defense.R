name: Tertiary FD Defense (15m PT)

on:
  workflow_dispatch:
  schedule:
    # Runs at :11/:26/:41/:56 each hour (UTC). We'll gate to LA hours below.
    - cron: "11,26,41,56 * * * *"

# Needed for Workload Identity Federation and repo access
permissions:
  id-token: write
  contents: read

jobs:
  run-tertiary-defense:
    runs-on: ubuntu-latest

    env:
      # ----- App config -----
      GCP_PROJECT: sac-vald-hub
      BQ_DATASET: analytics
      BQ_LOCATION: US

      # Email config (keep these for potential notifications)
      FROM_EMAIL: ${{ secrets.FROM_EMAIL }}
      TO_EMAIL:   ${{ secrets.TO_EMAIL }}
      SMTP_HOST:  ${{ secrets.SMTP_HOST }}
      SMTP_PORT:  ${{ secrets.SMTP_PORT }}
      SMTP_USER:  ${{ secrets.SMTP_USER }}
      SMTP_PASS:  ${{ secrets.SMTP_PASS }}

      VALD_CLIENT_ID:     ${{ secrets.VALD_CLIENT_ID }}
      VALD_CLIENT_SECRET: ${{ secrets.VALD_CLIENT_SECRET }}
      VALD_TENANT_ID:     ${{ secrets.VALD_TENANT_ID }}
      VALD_REGION:        use

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Echo UTC and LA time
        run: |
          echo "UTC now: $(date -u)"
          echo "LA  now: $(TZ=America/Los_Angeles date)"

      # ---- Gate by Pacific Time (same hours as main workflow, offset schedule) ----
      - name: Gate to 06:00–19:59 PT
        id: gate
        shell: bash
        run: |
          LA_HOUR=$(TZ=America/Los_Angeles date +%H)
          if [ "$LA_HOUR" -ge 6 ] && [ "$LA_HOUR" -lt 20 ]; then
            echo "outside_window=false" >> "$GITHUB_OUTPUT"
            echo "Proceeding (within 06:00–19:59 PT)."
          else
            echo "outside_window=true" >> "$GITHUB_OUTPUT"
            echo "Outside 06:00–19:59 PT – skipping."
          fi

      - name: Stop if outside LA window
        if: ${{ steps.gate.outputs.outside_window == 'true' }}
        run: echo "Skipping this tick (outside 06:00–19:59 PT)."

      # ---- Google auth (same WIF setup as main workflow) ----
      - name: Authenticate to Google Cloud (WIF)
        id: auth
        if: ${{ steps.gate.outputs.outside_window == 'false' }}
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/884700516106/locations/global/workloadIdentityPools/gha-pool/providers/github
          service_account: gha-bq@sac-vald-hub.iam.gserviceaccount.com
          create_credentials_file: true
          token_format: access_token
          access_token_lifetime: 3600s

      - name: Setup gcloud SDK
        if: ${{ steps.gate.outputs.outside_window == 'false' }}
        uses: google-github-actions/setup-gcloud@v2
        with:
          project_id: sac-vald-hub
          install_components: bq

      - name: Test GCP Authentication
        if: ${{ steps.gate.outputs.outside_window == 'false' }}
        run: |
          gcloud auth list
          bq ls --project_id="${GCP_PROJECT}" || echo "BigQuery test failed"

      # ---- R setup & deps ----
      - name: Setup R
        if: ${{ steps.gate.outputs.outside_window == 'false' }}
        uses: r-lib/actions/setup-r@v2

      - name: Install R packages (cached)
        if: ${{ steps.gate.outputs.outside_window == 'false' }}
        uses: r-lib/actions/setup-r-dependencies@v2
        with:
          use-public-rspm: true
          extra-packages: >
            any::bigrquery
            any::DBI
            any::dplyr
            any::tidyr
            any::readr
            any::stringr
            any::purrr
            any::tibble
            any::data.table
            any::hms
            any::lubridate
            any::httr
            any::jsonlite
            any::xml2
            any::curl
            any::gargle
            any::rlang
            any::lifecycle
            any::glue
            any::valdr

      # ---- Run the tertiary defense script at the new path ----
      - name: Run tertiary defense
        if: ${{ steps.gate.outputs.outside_window == 'false' }}
        run: |
          chmod +x .github/scripts/tertiary_fd_defense.R
          .github/scripts/tertiary_fd_defense.R
