# R on Databricks over SSH

Develop R from your own terminal or IDE while the compute, the data, and the governance stay in Databricks.
The R session runs on a Databricks cluster as **you**, so Unity Catalog permissions apply to everything it reads
and writes. The script you develop interactively is the same one that runs as a scheduled job.

The example reads the built-in `samples.nyctaxi.trips` table, aggregates fares by pickup hour with `dplyr` on Spark,
fits a linear model in R, and writes the result to a Unity Catalog table.

> The Databricks SSH tunnel is in **Beta**.

## What's in this repo

| File | Purpose |
|---|---|
| `explore.R` | Interactive walkthrough: connect, browse Unity Catalog, `dplyr` on Spark, model in R, write a table |
| `taxi_fares.R` | The production logic, `run_taxi_fares(sc, catalog)`; also runnable as `Rscript taxi_fares.R <catalog>` |
| `r_job.r` | Databricks notebook that the scheduled job runs; it `source()`s `taxi_fares.R` |
| `cluster.json` | Development cluster definition |
| `libraries.json` | R packages to install on the development cluster |
| `databricks.yml` | Bundle configuration: settings (catalog, machine type) and `dev` / `prod` targets |
| `resources/r_taxi_fares.job.yml` | The scheduled job that runs `r_job.r` |

## Recommended compute for R

| Use | Compute | Settings |
|---|---|---|
| Interactive development over SSH | Classic all-purpose cluster | **Dedicated** (single user) access mode, Databricks Runtime **17.3 LTS**, Unity Catalog, auto-termination on |
| Scheduled R jobs | Classic job compute (a new cluster per run) | **Dedicated** access mode, Databricks Runtime **17.3 LTS**; the cluster starts for the run and terminates afterwards |

Not supported for R:

- **Serverless compute** does not support R, in notebooks, jobs, or SSH sessions ([serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)).
- **Standard (shared) access mode** does not support R or SSH. Use dedicated access mode.
- **SQL warehouses** run SQL only.

Sizing:

- A **single-node** cluster with 4 cores and about 30 GB of memory is enough for development and small data
  (for example AWS `i3.xlarge` or `m5d.2xlarge`, Azure `Standard_D8ds_v5`, GCP `n2-standard-8`).
- Spark work (`dplyr` on Spark tables, `spark_write_table`) scales with **workers**. Add workers when the data grows.
- Anything you `collect()` into R lives in the **driver's memory**. If you collect large samples, choose a
  memory-optimized driver (for example AWS `r5d`, Azure `Standard_E*ds_v5`, GCP `n2-highmem`).
- Use **sparklyr**, not SparkR. SparkR is deprecated from Databricks Runtime 16.0.

## Prerequisites

- A Databricks workspace with Unity Catalog, and permission to create a cluster (or a dedicated cluster assigned to you).
- `CREATE SCHEMA` on a catalog you can write to, for the output table (the examples use `main`).
- The Databricks CLI. These steps were tested with v1.17.0.
- An SSH client: built into macOS, Linux, and Windows 10 and later.
- Optional: VS Code with the **Remote - SSH** extension (Microsoft).

## Setup (one time)

**1. Install the Databricks CLI**

```bash
# macOS
brew tap databricks/tap
brew install databricks

# Windows
winget install Databricks.DatabricksCLI

# Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

databricks --version
```

**2. Log in to your workspace** (opens a browser for single sign-on)

```bash
databricks auth login --host https://<your-workspace-url> --profile <profile-name>
```

**3. Create a development cluster**

Edit `cluster.json`: set `single_user_name` to your Databricks user email, and set `node_type_id` for your cloud
(see sizing above). Then:

```bash
databricks clusters create --json @cluster.json --profile <profile-name>
```

Note the `cluster_id` it returns. You can also create the cluster in the UI with the same settings.

**4. Install the R packages on the cluster**

Edit `libraries.json`: set `cluster_id`. Then:

```bash
databricks libraries install --json @libraries.json --profile <profile-name>
```

The packages are installed as **cluster libraries**, so they come back automatically every time the cluster starts.
They come from Posit Package Manager's prebuilt Linux binaries for Ubuntu 24.04 (`noble`), which is what Databricks
Runtime 17.x uses, so installation takes seconds instead of compiling from source.

**5. Create an SSH shortcut for the cluster**

```bash
databricks ssh setup --name r-dev --cluster <your-cluster-id> --profile <profile-name>
```

This adds a host called `r-dev` to `~/.ssh/config`. The connection runs through your Databricks login, so there
are no VPNs, open ports, or keys to manage.

**6. Test the connection**

```bash
ssh r-dev 'echo connected; R --version | head -1'
```

If the cluster is stopped, this starts it first, which takes 5–10 minutes.

## Choose how you work

All options run R on the same Databricks cluster, as you, with Unity Catalog permissions enforced.
They differ in where your code lives and how you send it to the cluster.

| Option | Best for | Where the code lives | Needs |
|---|---|---|---|
| [1. Terminal](#option-1-terminal) | Quick interactive work, no IDE | Copied to the cluster for the session | SSH only |
| [2. VS Code on the cluster](#option-2-vs-code-on-the-cluster) | Full IDE experience: editing, autocomplete, Cmd+Enter to run | On the cluster: a workspace folder (persists) or `/tmp` (temporary) | VS Code + Remote - SSH and R extensions |
| [3. Local editor + remote R](#option-3-local-editor--remote-r) | IDE look and feel when you can't install extensions | Your laptop | Any editor with a terminal |
| [4. Run one local script](#option-4-run-one-local-script) | Running a finished, self-contained script | Your laptop only; nothing is copied | SSH only |
| [5. Run a local multi-file project](#option-5-run-a-local-multi-file-project) | Running a project whose files `source()` each other | Your laptop; temporary copy on the cluster | SSH + `scp` |
| [6. Keep the code in the workspace](#option-6-keep-the-code-in-the-workspace) | Teams sharing code, files that survive cluster restarts | Databricks workspace (synced from your laptop or a Git folder) | Databricks CLI |
| [Scheduled job](#schedule-it-as-a-job) | Production runs | This repo, deployed to the workspace by the bundle | Databricks CLI |

**Suggested path:** start with option 1 or 2 to explore, keep the code in Git, and deploy it as a scheduled job
with the bundle. Options 4 and 5 are handy for running local code without storing anything in the workspace.

Before running, edit `catalog <- "main"` near the top of `explore.R` to a catalog where you can create a schema.

### Option 1: Terminal

```bash
scp explore.R taxi_fares.R r-dev:/tmp/       # copy the files to the cluster
ssh r-dev                                    # you are now on the cluster
cd /tmp
R                                            # start R; wait for the > prompt
```

At the `>` prompt, paste sections of `explore.R` one at a time, or run the whole file:

```r
source("explore.R", echo = TRUE, max.deparse.length = Inf)
```

Type `q("no")` to leave R and `exit` to leave the cluster. The first connection after a cluster start takes about
15 seconds while sparklyr prepares its environment; later connections are instant.

### Option 2: VS Code on the cluster

The whole VS Code window runs on the cluster: the file browser, terminal, and R session.

1. Install the **Remote - SSH** extension (Microsoft).
2. Command Palette → **Remote-SSH: Connect to Host** → `r-dev`. If asked for the platform, choose **Linux**.
3. **File → Open Folder**:
   - a workspace folder such as `/Workspace/Users/<your-email>/<project>` if the files should persist
     (see [option 6](#option-6-keep-the-code-in-the-workspace) for getting them there), or
   - `/tmp` for throwaway work (copy files there with `scp` as in option 1).
4. In the remote window, install the **R** extension (REditorSupport) with **Install in SSH: r-dev**.
   The `languageserver` package it needs was installed in setup step 4.
5. Open `explore.R`, run **R: Create R Terminal** from the Command Palette, and send lines with Cmd+Enter
   (Ctrl+Enter on Windows/Linux).

### Option 3: Local editor + remote R

Use this if you can't install VS Code extensions. The editor shows your local files; the R session runs on the cluster.

1. Open this repo's folder in VS Code (or any editor with a built-in terminal).
2. In the editor's terminal:

   ```bash
   scp explore.R taxi_fares.R r-dev:/tmp/
   ssh r-dev
   cd /tmp
   R
   ```

3. Select lines in `explore.R`, then Command Palette → **Terminal: Run Selected Text In Active Terminal**.
   This is built into VS Code, so no extension is needed. Copy and paste also works.

Re-run the `scp` line after you change a file that another file `source()`s.

### Option 4: Run one local script

```bash
ssh r-dev 'Rscript - main' < taxi_fares.R
```

SSH streams the file into R on the cluster, which runs it and keeps nothing. `Rscript -` reads the script from SSH,
and `main` is the catalog argument. This only works for self-contained scripts: a script that `source()`s other
files or reads local data files can't find them. Use option 5 for those.

### Option 5: Run a local multi-file project

```bash
scp explore.R taxi_fares.R r-dev:/tmp/
ssh r-dev 'cd /tmp && Rscript -e "source(\"explore.R\", echo = TRUE, max.deparse.length = Inf)"'
```

`scp` copies files to the cluster's local disk over the same SSH connection (`scp r-dev:/tmp/<file> .` copies results
back). `/tmp` on the cluster is wiped when the cluster restarts, so your local copy remains the only lasting one.
Re-run `scp` after you edit locally.

### Option 6: Keep the code in the workspace

Files in `/Workspace` persist across cluster restarts, are visible in the Databricks UI, and can be shared with
your team. Choose one:

- **Sync from your laptop:**

  ```bash
  databricks sync . /Workspace/Users/<your-email>/<project> --profile <profile-name>          # one-off copy
  databricks sync . /Workspace/Users/<your-email>/<project> --watch --profile <profile-name>  # keep syncing while you edit
  ```

- **Git folder:** in the workspace, **Workspace → Create → Git folder** and paste this repo's URL. Pull and push
  from the UI.

Then work in that folder with option 1 (`cd /Workspace/Users/<your-email>/<project>` instead of `/tmp`) or option 2.

## Schedule it as a job

The job is deployed with a **Databricks Asset Bundle (DAB)**. Its definition lives in this repo as code
(`databricks.yml` and `resources/r_taxi_fares.job.yml`), and one command creates or updates it in any workspace,
the same way every time.

The job runs the `r_job.r` notebook, which `source()`s the same `taxi_fares.R` you run over SSH. It runs on job
compute: a new cluster starts for each run and terminates afterwards.

**1. Set the variables.** Change the defaults in `databricks.yml`, or pass `--var name=value` to each command:

| Variable | Default | Set it to |
|---|---|---|
| `catalog` | `main` | A catalog where you can create a schema |
| `node_type_id` | `i3.xlarge` | A machine type for your cloud (see [sizing](#recommended-compute-for-r)) |

**2. Validate, deploy, and run** (from the repo folder):

```bash
databricks bundle validate -t dev --profile <profile-name>
databricks bundle deploy   -t dev --profile <profile-name>
databricks bundle run r_taxi_fares -t dev --profile <profile-name>     # runs once and waits for the result
```

`deploy` uploads the repo files to your bundle folder,
`/Workspace/Users/<your-email>/.bundle/r-databricks-ssh-quickstart/<target>`, and creates or updates the job.
Re-run `deploy` after every code change.

**3. Deploy to production** with the same commands and `-t prod`.

| Target | Job name | Schedule |
|---|---|---|
| `dev` (default) | Prefixed with `[dev <your-name>]`, so you can test changes without touching production | Always paused |
| `prod` | `r-taxi-fares-by-hour` | As defined in `resources/r_taxi_fares.job.yml` |

The schedule (daily 06:00 UTC) is defined as **paused**. When you're ready, change `pause_status` to `UNPAUSED` in
`resources/r_taxi_fares.job.yml` and deploy `prod` again. For production, deploy from CI (for example GitHub Actions)
so the job always matches the main branch.

Notes:

- In notebooks and jobs, sparklyr connects with `spark_connect(method = "databricks")`. Over SSH, the scripts use
  `method = "databricks_connect"`. `taxi_fares.R` doesn't connect itself when it is `source()`d, so the same file
  works in both places.
- If the job needs extra CRAN packages, add them to the task in `resources/r_taxi_fares.job.yml`:

  ```yaml
  libraries:
    - cran:
        package: <package>
        repo: https://packagemanager.posit.co/cran/__linux__/noble/latest
  ```
