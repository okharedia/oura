#!/usr/bin/env python3
"""One-time OAuth2 setup: authorizes with Oura, stores tokens in Secret Manager.

Usage:
    1. Register an app at https://cloud.ouraring.com/oauth/applications
       - Set redirect URI to http://localhost:8080/callback
    2. Set environment variables:
         export OURA_CLIENT_ID=<your_client_id>
         export OURA_CLIENT_SECRET=<your_client_secret>
         export GCP_PROJECT_ID=<your_gcp_project>
    3. Run: python setup_oauth.py
"""

import os
import subprocess
import sys
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode, urlparse, parse_qs

import requests

CLIENT_ID = os.environ.get("OURA_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("OURA_CLIENT_SECRET", "")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "oura-sync")
REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "email personal daily heartrate workout tag session spo2"

AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
TOKEN_URL = "https://api.ouraring.com/oauth/token"


def gcloud_secret_create(name: str):
    """Ensure a secret exists using gcloud CLI."""
    result = subprocess.run(
        ["gcloud", "secrets", "describe", name, "--project", PROJECT_ID],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        subprocess.run(
            ["gcloud", "secrets", "create", name, "--project", PROJECT_ID,
             "--replication-policy", "automatic"],
            check=True, capture_output=True, text=True,
        )
        print(f"  Created secret: {name}")


def gcloud_secret_set(name: str, value: str):
    """Store a value as a new secret version using gcloud CLI."""
    subprocess.run(
        ["gcloud", "secrets", "versions", "add", name, "--project", PROJECT_ID,
         "--data-file", "-"],
        input=value, check=True, capture_output=True, text=True,
    )
    print(f"  Stored: {name}")


class CallbackHandler(BaseHTTPRequestHandler):
    """Handles the OAuth2 callback."""

    auth_code: str | None = None

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/callback":
            params = parse_qs(parsed.query)
            CallbackHandler.auth_code = params.get("code", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h2>Authorization successful! You can close this tab.</h2>")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress logs


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("Error: Set OURA_CLIENT_ID and OURA_CLIENT_SECRET environment variables.")
        sys.exit(1)

    # Step 1: Open browser for authorization
    auth_params = urlencode({
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPES,
    })
    auth_url = f"{AUTH_URL}?{auth_params}"
    print(f"Opening browser for authorization...\n  {auth_url}")
    webbrowser.open(auth_url)

    # Step 2: Wait for callback
    server = HTTPServer(("localhost", 8080), CallbackHandler)
    print("Waiting for callback on http://localhost:8080/callback ...")
    server.handle_request()

    code = CallbackHandler.auth_code
    if not code:
        print("Error: No authorization code received.")
        sys.exit(1)
    print(f"Received authorization code.")

    # Step 3: Exchange code for tokens
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }, timeout=30)
    resp.raise_for_status()
    tokens = resp.json()
    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]
    print("Tokens obtained successfully.")

    # Step 4: Verify token works
    verify = requests.get(
        "https://api.ouraring.com/v2/usercollection/personal_info",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    verify.raise_for_status()
    info = verify.json()
    print(f"Verified: connected as {info.get('email', 'unknown')}")

    # Step 5: Store in Secret Manager (via gcloud CLI)
    print("\nStoring tokens in Secret Manager...")
    for name in ["oura-access-token", "oura-refresh-token", "oura-client-id", "oura-client-secret"]:
        gcloud_secret_create(name)
    gcloud_secret_set("oura-access-token", access_token)
    gcloud_secret_set("oura-refresh-token", refresh_token)
    gcloud_secret_set("oura-client-id", CLIENT_ID)
    gcloud_secret_set("oura-client-secret", CLIENT_SECRET)

    print("\nSetup complete! You can now deploy the Cloud Function.")


if __name__ == "__main__":
    main()
