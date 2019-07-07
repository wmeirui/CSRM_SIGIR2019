import csv
import time
import pickle
import operator

# Load the yoochoose dataset
with open("data/yoochoose-clicks.csv", "rb") as f:
    reader = csv.DictReader(f, delimiter=',')
    sess_clicks = {}
    sess_date = {}
    ctr = 0
    curid = -1
    curdate = None
    for data in reader:
        sessid = data['SessionId']
        if curdate and not curid == sessid:
            date = time.mktime(time.strptime(curdate, '%Y-%m-%dT%H:%M:%S.%fZ'))
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
            print ('Loaded', ctr)
    date = time.mktime(time.strptime(curdate, '%Y-%m-%dT%H:%M:%S.%fZ'))
    sess_date[curid] = date

# Filter out length 1 sessions
for s in sess_clicks.keys():
    if len(sess_clicks[s]) == 1:
        del sess_clicks[s]
        del sess_date[s]

# Count number of times each item appears
iid_counts = {}
for s in sess_clicks:
    seq = sess_clicks[s]
    for iid in seq:
        if iid_counts.has_key(iid):
            iid_counts[iid] += 1
        else:
            iid_counts[iid] = 1


sorted_counts = sorted(iid_counts.items(), key=operator.itemgetter(1))

for s in sess_clicks.keys():
    curseq = sess_clicks[s]
    filseq = filter(lambda i: iid_counts[i] >= 5, curseq)
    if len(filseq) < 2:
        del sess_clicks[s]
        del sess_date[s]
    else:
        sess_clicks[s] = filseq

# Split out test set based on dates
dates = sess_date.items()
maxdate = dates[0][1]

for _, date in dates:
    if maxdate < date:
        maxdate = date

# maxdate = 30/09/2014, 2, 59, 59, 430000

splitdate = maxdate - 86400
print('Split date', splitdate)
train_full_sess = filter(lambda x: x[1] < splitdate, dates)
test_sess = filter(lambda x: x[1] >= splitdate, dates)

# Sort sessions by date
train_full_sess = sorted(train_full_sess, key=operator.itemgetter(1))
test_sess = sorted(test_sess, key=operator.itemgetter(1))

splitdate_2 = train_full_sess[-1][1] - 86400
print('Split date2', splitdate_2)
train_sess = filter(lambda x: x[1] < splitdate_2, train_full_sess)
valid_sess = filter(lambda x: x[1] >= splitdate_2, train_full_sess)
train_sess = sorted(train_sess, key=operator.itemgetter(1))
valid_sess = sorted(valid_sess, key=operator.itemgetter(1))

# Choosing item count >=5 gives approximately the same number of items as reported in paper
item_dict = {}
item_ctr = 1
train_seqs = []
train_dates = []
# Convert training sessions to sequences and renumber items to start from 1
for s, date in train_sess:
    seq = sess_clicks[s]
    outseq = []
    for i in seq:
        if item_dict.has_key(i):
            outseq += [item_dict[i]]
        else:
            outseq += [item_ctr]
            item_dict[i] = item_ctr
            item_ctr += 1
    if len(outseq) < 2:  # Doesn't occur
        continue
    train_seqs += [outseq]      # 7953884
    train_dates += [date]

train_seqs_len = len(train_seqs)
print('train_seqs_len', train_seqs_len)
train_1_4 = train_seqs[5965413:]  # 1988471
print len(train_1_4)
train_1_64 = train_seqs[7829605:]  # 124279
print len(train_1_64)

print('num of items:' + str(item_ctr-1))   # 37483
print('num of sessions:' + str(len(train_seqs)+len(valid_sess)+len(test_sess)))  # 7981581

################################################experient dataset##########################################
item_dict1 = {}
for seqss in train_1_4:
    for itemss in seqss:
        if item_dict1.has_key(itemss):
            item_dict1[itemss] += 0
        else:
            item_dict1[itemss] = 1
print(str(len(item_dict1)) + 'len item_dict4')  # 30672
item_dict2 = {}
for seqss2 in train_1_64:
    for itemss2 in seqss2:
        if item_dict2.has_key(itemss2):
            item_dict2[itemss2] += 0
        else:
            item_dict2[itemss2] = 1
print(str(len(item_dict2)) + 'len item_dict64')  # 17702
print('num_items: ' + str(item_ctr - 1))

valid_seqs1 = []
valid_dates1 = []
# Convert test sessions to sequences, ignoring items that do not appear in training set
for s1, date1 in valid_sess:
    seq1 = sess_clicks[s1]
    outseq1 = []
    for i in seq1:
        if item_dict.has_key(i):
            ii = item_dict[i]
            if item_dict1.has_key(ii):
                outseq1 += [ii]
    if len(outseq1) < 2:
        continue
    valid_seqs1 += [outseq1]
    valid_dates1 += [date1]
print('num of original yoo4_valid sequences: ' + str(len(valid_seqs1))) # 12371

test_seqs1 = []
test_dates1 = []
# Convert test sessions to sequences, ignoring items that do not appear in training set
for s1, date1 in test_sess:
    seq1 = sess_clicks[s1]
    outseq1 = []
    for i in seq1:
        if item_dict.has_key(i):
            ii = item_dict[i]
            if item_dict1.has_key(ii):
                outseq1 += [ii]
    if len(outseq1) < 2:
        continue
    test_seqs1 += [outseq1]
    test_dates1 += [date1]
print('num of original yoo4_test sequences: ' + str(len(test_seqs1))) # 15317

valid_seqs2 = []
valid_dates2 = []
# Convert test sessions to sequences, ignoring items that do not appear in training set
for s2, date2 in valid_sess:
    seq2 = sess_clicks[s2]
    outseq2 = []
    for i in seq2:
        if item_dict.has_key(i):
            ii = item_dict[i]
            if item_dict2.has_key(ii):
                outseq2 += [ii]
    if len(outseq2) < 2:  # Doesn't occur
        continue
    valid_seqs2 += [outseq2]
    valid_dates2 += [date2]
print('num of original yoo64_valid sequences: ' + str(len(valid_seqs2)))  # 12309

test_seqs2 = []
test_dates2 = []
# Convert test sessions to sequences, ignoring items that do not appear in training set
for s2, date2 in test_sess:
    seq2 = sess_clicks[s2]
    outseq2 = []
    for i in seq2:
        if item_dict.has_key(i):
            ii = item_dict[i]
            if item_dict2.has_key(ii):
                outseq2 += [ii]
    if len(outseq2) < 2:  # Doesn't occur
        continue
    test_seqs2 += [outseq2]
    test_dates2 += [date2]
print('num of original yoo64_test sequences: ' + str(len(test_seqs2)))  # 15236

def process_seqs(iseqs):
    out_seqs = []
    labs = []
    for seq in iseqs:
        for i in range(len(seq)-1):
            out_seqs +=[seq[:i+1]]
            labs += [seq[i+1]]
    return out_seqs, labs

tr_seqs4, tr_labs4 = process_seqs(train_1_4)
print('num of yoo4 train sessions:' + str(len(tr_seqs4)))  # 6135750
tr_seqs64, tr_labs64 = process_seqs(train_1_64)
print('num of yoo64 train sessions:' + str(len(tr_seqs64)))  # 384464
valid_seqs4, valid_labs4 = process_seqs(valid_seqs1)
print('num of yoo4 valid sessions:' + str(len(valid_seqs4)))  # 45845
valid_seqs64, valid_labs64 = process_seqs(valid_seqs2)
print('num of yoo64 valid sessions:' + str(len(valid_seqs64))) # 45523
te_seqs4, te_labs4 = process_seqs(test_seqs1)
print('num of yoo4 test sessions:' + str(len(te_seqs4)))  # 55863
te_seqs64, te_labs64 = process_seqs(test_seqs2)
print('num of yoo64 test sessions:' + str(len(te_seqs64)))  # 55415

pickle.dump((tr_seqs4, tr_labs4), open('data/yoo4_train.pkl', 'w'))
pickle.dump((tr_seqs64, tr_labs64), open('data/yoo64_train.pkl', 'w'))
pickle.dump((valid_seqs4, valid_labs4), open('data/yoo4_valid.pkl', 'w'))
pickle.dump((valid_seqs64, valid_labs64), open('data/yoo64_valid.pkl', 'w'))
pickle.dump((te_seqs4, te_labs4), open('data/yoo4_test.pkl', 'w'))
pickle.dump((te_seqs64, te_labs64), open('data/yoo64_test.pkl', 'w'))

print('Done!')
