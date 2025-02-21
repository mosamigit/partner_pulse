
### Steps to run
1. Make sure that you have Python 3 is installed.
2. Install requirement.txt 
   ```
   <<python>> -m pip install -r requirements.txt
   ```
3. Running the script
   1. DEV 
      ```
      <<python>> app.py
      ```
   2. PROD 
      ```
      <<python>> -m waitress --port 9000 wsgi:application
      ```
