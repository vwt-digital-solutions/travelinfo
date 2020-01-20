from flask import request

def travelinfo_get():  # noqa: E501
    return travelrequests, 200


def travelrequest_post():  # noqa: E501
    print(request.json)
    new_req = request.json

    for i,req in enumerate(travelrequests):
        print ("print lijst {}".format(req))
        print("print nieuw {}".format(new_req))
        if req['departure'] >= new_req['departure']:
            travelrequests.insert(i, new_req)
            break
        elif i+1 == len(travelrequests):
            travelrequests.append(new_req)
            break

    if not travelrequests:
        travelrequests.append(new_req)

    return travelrequests, 201

travelrequests = []