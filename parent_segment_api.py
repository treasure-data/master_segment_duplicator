import json


def createParentSegment(client, body):
    try:
        URL = f"/audiences"
        response = client.request("POST", URL, json=body)
        # data = json.loads(response.text)
        # response.raise_for_status()
        # if response.ok:
        # print("createParentSegment good")
        return response.get("id"), response.get("name"), "created"
    except Exception as ex:
        # print(f"Post API create Parent Segment failed: {response.text}")
        # raise Exception(f'Post API create Parent Segment failed: {ex}')
        # print(str(ex), URL, body, response.text)
        print(
            "createParentSegment exception: ",
            str(ex),
            URL,
            body,
            response if response else "No Response",
        )
        raise


def updateParentSegment(client, body, id):
    try:
        URL = f"/audiences/{id}"
        response = client.request("PUT", URL, json=body)
        # data = json.loads(response.text)
        # response.raise_for_status()
        # if response.ok:
        #     print("updateParentSegment good")
        #     return data.get("id"), data.get("name"), "updated"
        return response.get("id"), response.get("name"), "updated"
    except Exception as ex:
        # print(f"Post API create Parent Segment failed: {response.text}")
        # raise Exception(f'Post API create Parent Segment failed: {ex}')
        print(
            "updateParentSegment exception: ",
            str(ex),
            URL,
            body,
            response if response else "No Response",
        )
        raise


def deleteParentSegment(client, id):
    try:
        URL = f"/audiences/{id}"
        response = client.request("DELETE", URL)
        # data = json.loads(response.text)
        # response.raise_for_status()
        # if response.ok:
        #     print("deleteParentSegment good")
        #     return data.get("id"), data.get("name"), "updated"
        return response.get("id"), response.get("name"), "updated"
    except Exception as ex:
        # print(f"Post API create Parent Segment failed: {response.text}")
        # raise Exception(f'Post API create Parent Segment failed: {ex}')
        # print(str(ex), URL, None, response.text)
        print(
            "deleteParentSegment exception: ",
            str(ex),
            URL,
            None,
            response if response else "No Response",
        )
        raise


def getParentSegment(client, body):
    try:
        URL = f"/audiences"
        response = client.request("GET", URL)
        # data = json.loads(response.text)
        _body = json.loads(body)
        print(_body)
        # response.raise_for_status()
        # if response.ok:
        for row in response:
            print(row["id"])
            print(_body["id"])
            if _body["id"] and str(row["id"]) == str(_body["id"]):
                print(f"Found Parent Segment by id {row.get('id')}")
                return (row.get("id"), row.get("name"), "selected")
            elif (
                not _body["id"]
                and _body["name"]
                and str(row["name"]) == str(_body["name"])
            ):
                print(f"Found Parent Segment by name {row.get('id')}")
                return (row.get("id"), row.get("name"), "selected")
            elif not _body["id"] and not _body["name"]:
                print(
                    "Atleast name or id should be provide in the parent segment template yml file."
                )
                break
        # No Matches
        print(f"Not Found Parent Segment {_body['id']}")
        return None, None, "selected"
    except Exception as ex:
        # print(f"Post API create Parent Segment failed: {response.text}")
        # raise Exception(f'Post API create Parent Segment failed: {ex}')
        # print(str(ex), URL, None, response.text)
        print(
            "getParentSegment exception: ",
            str(ex),
            URL,
            None,
            response if response else "No Response",
        )
        raise


def ps_check_and_update(client, body):
    # check if parent segment exists
    # if exists return the audience_id
    # and update the parent segment
    audience_id, name, message = getParentSegment(client, body)
    try:
        if audience_id:
            audience_id, name, message = updateParentSegment(client, body, audience_id)
        elif not audience_id:
            audience_id, name, message = createParentSegment(client, body)
        return audience_id, name, message
    except Exception as ex:
        print("ps_check_and_update exception for create an update: ", str(ex))
        # print("ps_check_and_update fianlly for create an update: ")
        _audience_id, _name, _message = deleteParentSegment(client, audience_id)
        audience_id, name, message = createParentSegment(client, body)
        return audience_id, name, message
