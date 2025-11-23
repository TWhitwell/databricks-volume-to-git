#!/usr/bin/env python3
"""
Databricks to Git Pipeline

Downloads files from Databricks Volumes and commits changes to GitHub.

Required environment variables:
    GITHUB_REPO       - e.g., github.com/user/repo.git
    GITHUB_PAT        - GitHub personal access token
    DATABRICKS_HOST   - e.g., https://your-workspace.azuredatabricks.net
    DATABRICKS_TOKEN  - Databricks personal access token
    VOLUME_PATH       - Source path in Databricks (e.g., /Volumes/workspace/default/pipeline-v1/logs)

Optional environment variables:
    BRANCH_NAME       - Git branch (default: main)
    LOCAL_FOLDER      - Local repo path (default: ./repo)
    DESTINATION_FOLDER - Folder in repo for files (default: logs)
    LOG_DIR           - Log directory (default: ./logs)
"""

import os
import sys
import hashlib
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import requests
from dotenv import load_dotenv

# Load .env file
load_dotenv()


# ===========================================
# CONFIG
# ===========================================
def get_config() -> dict:
    required = ["GITHUB_REPO", "GITHUB_PAT", "DATABRICKS_HOST", "DATABRICKS_TOKEN", "VOLUME_PATH"]
    missing = [var for var in required if not os.environ.get(var)]
    
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")
    
    return {
        "github_repo": os.environ["GITHUB_REPO"],
        "github_pat": os.environ["GITHUB_PAT"],
        "databricks_host": os.environ["DATABRICKS_HOST"].rstrip("/"),
        "databricks_token": os.environ["DATABRICKS_TOKEN"],
        "volume_path": os.environ["VOLUME_PATH"].rstrip("/"),
        "branch_name": os.environ.get("BRANCH_NAME", "main"),
        "local_folder": Path(os.environ.get("LOCAL_FOLDER", "./repo")),
        "destination_folder": os.environ.get("DESTINATION_FOLDER", "logs"),
        "log_dir": Path(os.environ.get("LOG_DIR", "./logs")),
    }


# ===========================================
# LOGGING SETUP
# ===========================================
def setup_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"databricks_to_git_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return log_file


# ===========================================
# CHECKSUM TRACKER
# ===========================================
class ChecksumTracker:
    def __init__(self, checksum_file: Path):
        self.checksum_file = checksum_file
        self.old_checksums: Dict[str, str] = {}
        self.new_checksums: Dict[str, str] = {}
        self._load()
    
    def _load(self):
        if self.checksum_file.exists():
            with open(self.checksum_file, "r") as f:
                for line in f:
                    if "=" in line:
                        file_path, checksum = line.strip().split("=", 1)
                        self.old_checksums[file_path] = checksum
    
    def save(self):
        with open(self.checksum_file, "w") as f:
            for file_path, checksum in self.new_checksums.items():
                f.write(f"{file_path}={checksum}\n")
    
    @staticmethod
    def get_checksum(content: bytes) -> str:
        return hashlib.md5(content).hexdigest()
    
    def has_changed(self, relative_path: str, checksum: str) -> bool:
        self.new_checksums[relative_path] = checksum
        old = self.old_checksums.get(relative_path)
        return old is None or old != checksum


# ===========================================
# DATABRICKS DOWNLOADER
# ===========================================
class DatabricksDownloader:
    def __init__(self, host: str, token: str, volume_path: str):
        self.host = host
        self.token = token
        self.volume_path = volume_path.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
    
    def list_files(self, path: str = "") -> List[dict]:
        """Recursively list all files in the volume path"""
        all_files = []
        
        try:
            list_url = f"{self.host}/api/2.0/fs/directories{self.volume_path}/{path}"
            response = requests.get(list_url, headers=self.headers)
            response.raise_for_status()
            
            contents = response.json().get("contents", [])
            
            for item in contents:
                if item.get("is_dir"):
                    # Recursively list subdirectories
                    subpath = f"{path}/{item['name']}" if path else item["name"]
                    all_files.extend(self.list_files(subpath))
                else:
                    # It's a file
                    file_path = f"{path}/{item['name']}" if path else item["name"]
                    all_files.append({
                        "path": file_path,
                        "size": item.get("file_size", 0)
                    })
            
            return all_files
            
        except requests.RequestException as e:
            logging.error(f"Failed to list files at {path}: {e}")
            return []
    
    def download_file(self, file_path: str) -> bytes:
        """Download a file from the volume"""
        try:
            download_url = f"{self.host}/api/2.0/fs/files{self.volume_path}/{file_path}"
            response = requests.get(download_url, headers=self.headers)
            response.raise_for_status()
            return response.content
            
        except requests.RequestException as e:
            logging.error(f"Failed to download {file_path}: {e}")
            return None


# ===========================================
# GIT OPERATIONS
# ===========================================
def git_setup(config: dict) -> bool:
    """Clone or pull the repo"""
    repo_url = f"https://{config['github_pat']}@{config['github_repo']}"
    local_folder = config["local_folder"]
    branch = config["branch_name"]
    
    try:
        if local_folder.exists():
            logging.info("Repo exists, pulling latest changes...")
            subprocess.run(
                ["git", "-C", str(local_folder), "remote", "set-url", "origin", repo_url],
                check=True, capture_output=True
            )
            subprocess.run(
                ["git", "-C", str(local_folder), "fetch", "origin", branch],
                check=True, capture_output=True
            )
            subprocess.run(
                ["git", "-C", str(local_folder), "reset", "--hard", f"origin/{branch}"],
                check=True, capture_output=True
            )
        else:
            logging.info("Cloning repo...")
            subprocess.run(
                ["git", "clone", "-b", branch, repo_url, str(local_folder)],
                check=True, capture_output=True
            )
        
        logging.info("Git setup complete")
        return True
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Git operation failed: {e.stderr.decode() if e.stderr else str(e)}")
        return False


def git_commit_and_push(config: dict, changed_files: int) -> bool:
    """Commit and push changes"""
    local_folder = config["local_folder"]
    
    try:
        # Add all changes in the destination folder (use relative path)
        subprocess.run(
            ["git", "-C", str(local_folder), "add", config["destination_folder"]],
            check=True, capture_output=True
        )
        
        # Check if there are changes to commit
        status = subprocess.run(
            ["git", "-C", str(local_folder), "status", "--porcelain"],
            check=True, capture_output=True, text=True
        )
        
        if not status.stdout.strip():
            logging.info("No changes to commit")
            return True
        
        # Commit
        commit_msg = f"Update {config['destination_folder']}: {changed_files} file(s) changed"
        subprocess.run(
            ["git", "-C", str(local_folder), "commit", "-m", commit_msg],
            check=True, capture_output=True
        )
        
        # Push
        subprocess.run(
            ["git", "-C", str(local_folder), "push", "origin", config["branch_name"]],
            check=True, capture_output=True
        )
        
        logging.info(f"Committed and pushed: {commit_msg}")
        return True
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Git commit/push failed: {e.stderr.decode() if e.stderr else str(e)}")
        return False


# ===========================================
# MAIN PIPELINE
# ===========================================
def main():
    try:
        config = get_config()
    except EnvironmentError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    log_file = setup_logging(config["log_dir"])
    logging.info("=== Databricks to Git Pipeline started ===")
    logging.info(f"Log file: {log_file}")
    
    # Step 1: Setup Git repo
    logging.info("Step 1: Setting up Git repository")
    if not git_setup(config):
        logging.error("Git setup failed, aborting")
        sys.exit(1)
    
    # Step 2: List files in Databricks
    logging.info("Step 2: Listing files in Databricks Volume")
    downloader = DatabricksDownloader(
        config["databricks_host"],
        config["databricks_token"],
        config["volume_path"]
    )
    
    files = downloader.list_files()
    logging.info(f"Found {len(files)} files in Volume")
    
    if not files:
        logging.warning("No files found in Volume")
        sys.exit(0)
    
    # Step 3: Download and save changed files
    logging.info("Step 3: Downloading changed files")
    
    checksum_file = config["log_dir"] / ".checksums_databricks"
    tracker = ChecksumTracker(checksum_file)
    
    dest_folder = config["local_folder"] / config["destination_folder"]
    dest_folder.mkdir(parents=True, exist_ok=True)
    
    downloaded, skipped, failed = 0, 0, 0
    
    for file_info in files:
        file_path = file_info["path"]
        
        # Download file content
        content = downloader.download_file(file_path)
        
        if content is None:
            logging.error(f"Failed to download: {file_path}")
            failed += 1
            continue
        
        checksum = tracker.get_checksum(content)
        
        if tracker.has_changed(file_path, checksum):
            # Save to local folder
            local_file = dest_folder / file_path
            local_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(local_file, "wb") as f:
                f.write(content)
            
            logging.info(f"Downloaded: {file_path}")
            downloaded += 1
        else:
            logging.info(f"Skipped (unchanged): {file_path}")
            skipped += 1
    
    tracker.save()
    
    # Step 4: Commit and push to Git
    if downloaded > 0:
        logging.info("Step 4: Committing and pushing to Git")
        if not git_commit_and_push(config, downloaded):
            logging.error("Git commit/push failed")
            sys.exit(1)
    else:
        logging.info("No new files to commit")
    
    # Summary
    logging.info("=== Pipeline complete ===")
    logging.info(f"Downloaded: {downloaded} | Skipped: {skipped} | Failed: {failed}")
    
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()