# Testing scripts, don't actually rely on this.

run-dev *args:
    cargo b
    sudo target/debug/stratum {{args}}