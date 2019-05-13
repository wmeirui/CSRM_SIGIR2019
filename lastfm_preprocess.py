import csv
import time
import pickle
import operator
from collections import Counter

# Load the lastfm dataset
with open("data/lastfm_info.csv", "rb") as f:
    reader = csv.DictReader(f, delimiter=',')
    sess_clicks = {}
    sess_date = {}
    ctr = 0
    curid = -1
    curdate = None
    for data in reader:
        sessid = data['SessionId']
        if curdate and not curid == sessid:
            date = time.mktime(time.strptime(curdate, '%Y-%m-%dT%H:%M:%SZ'))
            sess_date[curid] = date
        curid = sessid
        item = data['ItemId']
        curdate = data['TimeStamp']
        if sess_clicks.has_key(sessid):
            sess_clicks[sessid] += [item]
        else:
            sess_clicks[sessid] = [item]
        ctr += 1
        if ctr % 100000 == 0:
            print('Loaded', ctr)
    date = time.mktime(time.strptime(curdate, '%Y-%m-%dT%H:%M:%SZ'))
    sess_date[curid] = date

# collapse repeating items
new_sess_clicks = {}
for s in sess_clicks.keys():
    new_sess_clicks[s] = [sess_clicks[s][0]]
    for i in range(1, len(sess_clicks[s])):
        last_event = new_sess_clicks[s][-1]
        current_event = sess_clicks[s][i]
        if current_event != last_event:
            new_sess_clicks[s].append(current_event)

sess_clicks = new_sess_clicks

# Filter out length 1 and >50 sessions
for s in sess_clicks.keys():
    if len(sess_clicks[s]) == 1 or len(sess_clicks[s]) > 50:
        del sess_clicks[s]
        del sess_date[s]


# Split out test set based on dates
dates = sess_date.items()
maxdate = dates[0][1]

for _, date in dates:
    if maxdate < date:
        maxdate = date

splitdate = maxdate - 60 * 86400
print('Split date', splitdate)
train_full_sess = filter(lambda x: x[1] < splitdate, dates)
test_sess = filter(lambda x: x[1] >= splitdate, dates)

# Sort sessions by date
train_full_sess = sorted(train_full_sess, key=operator.itemgetter(1))    # 275860
test_sess = sorted(test_sess, key=operator.itemgetter(1))   # 5794

splitdate_2 = train_full_sess[-1][1] - 21 * 86400
print('Split date2', splitdate_2)
train_sess = filter(lambda x: x[1] < splitdate_2, train_full_sess)
valid_sess = filter(lambda x: x[1] >= splitdate_2, train_full_sess)
train_sess = sorted(train_sess, key=operator.itemgetter(1))
valid_sess = sorted(valid_sess, key=operator.itemgetter(1))

item_dict_new = {}
item_ctr_new = 1
train_seqs = []
train_dates = []
# Convert training sessions to sequences and renumber items to start from 1
for s, date in train_sess:
    seq = sess_clicks[s]
    outseq = []
    for i in seq:
        if item_dict_new.has_key(i):
            outseq += [item_dict_new[i]]
        else:
            outseq += [item_ctr_new]
            item_dict_new[i] = item_ctr_new
            item_ctr_new += 1
    if len(outseq) < 2:  # Doesn't occur
        continue
    train_seqs += [outseq]
    train_dates += [date]
print('num of train sequences:' + str(len(train_seqs)))  # 269847

valid_seqs = []
valid_dates = []
# Convert valid sessions to sequences, ignoring items that do not appear in training set
for s, date in valid_sess:
    seq = sess_clicks[s]
    outseq = []
    for i in seq:
        if item_dict_new.has_key(i):
            outseq += [item_dict_new[i]]
    if len(outseq) < 2:  # Doesn't occur
        continue
    valid_seqs += [outseq]
    valid_dates += [date]
print('num of valid sequences:' + str(len(valid_seqs)))  # 5996

test_seqs = []
test_dates = []
# Convert test sessions to sequences, ignoring items that do not appear in training set
for s, date in test_sess:
    seq = sess_clicks[s]
    outseq = []
    for i in seq:
        if item_dict_new.has_key(i):
            outseq += [item_dict_new[i]]
    if len(outseq) < 2:  # Doesn't occur
        continue
    test_seqs += [outseq]
    test_dates += [date]
print('num of test sequences:' + str(len(test_seqs)))  # 5771

print('num of used items:' + str(item_ctr_new - 1))   # 39163
print('num of sessions:' + str(len(train_seqs)+len(valid_seqs)+len(test_seqs)))  # 281614

################################################experient dataset##########################################
def process_seqs(iseqs):
    out_seqs = []
    labs = []
    for seq in iseqs:
        for i in range(len(seq)-1):
            out_seqs +=[seq[:i+1]]
            labs += [seq[i+1]]
    return out_seqs, labs

tr_seqs, tr_labs = process_seqs(train_seqs)
print('num of lastfm train sessions:' + str(len(tr_seqs)))  # 3380957
valid_seqs, valid_labs = process_seqs(valid_seqs)
print('num of lastfm valid sessions:' + str(len(valid_seqs)))  # 73923
te_seqs, te_labs = process_seqs(test_seqs)
print('num of lastfm test sessions:' + str(len(te_seqs)))  # 68428

pickle.dump((tr_seqs, tr_labs), open('data/lastfm_train.pkl', 'w'))
pickle.dump((valid_seqs, valid_labs), open('data/lastfm_valid.pkl', 'w'))
pickle.dump((te_seqs, te_labs), open('data/lastfm_test.pkl', 'w'))

print('Done!')
