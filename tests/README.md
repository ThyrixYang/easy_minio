
Run with standard output
```
pytest -s .
```

Run with converage report 
```
pytest --cov=easy_minio .
```

Run single test file
```
pytest test_speed.py
```

Run specific function
```
pytest test_usable.py -k 'auto_refresh'
```