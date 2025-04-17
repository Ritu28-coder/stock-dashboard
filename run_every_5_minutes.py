import time
import subprocess

# Run the insert script every 5 minutes
while True:
    print("🔁 Running insert_multiple.py...")
    
    # Run your stock data script
    subprocess.run(["python3", "insert_multiple.py"])
    
    print("✅ Done. Waiting 5 minutes...\n")
    
    # Wait 5 minutes (300 seconds)
    time.sleep(300)
