# Callback function
# Calculates new elo for a player
def updateElo(record):
    keyTuple, _, bins = record
    # Add score to elo
    bins["elo"] = bins["elo"] + bins["score"]
    client.put(keyTuple, bins)

query.foreach(updateElo)

# Player elos should be updated
brs = client.batch_read(keyTuples)
for br in brs.batch_records:
    # Print record bin
    print(br.record[2])
# {'score': 100, 'elo': 1500}
# {'score': 20, 'elo': 1520}
# {'score': 10, 'elo': 1110}
# {'score': 200, 'elo': 1100}
