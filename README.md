build vm
```bash
vagrant up
curl http://127.0.0.1:8098/ping     # should return 'OK' if everything worked
```

connect to vm and run scripts
```bash
vagrant ssh

# load data
python /vagrant/src/main.py load

# run queries
python /vagrant/src/main.py query
```
