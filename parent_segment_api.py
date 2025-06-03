import logging

logger = logging.getLogger(__name__)


def createParentSegment(client, body):
    try:
        response = None
        URL = f"/audiences"
        response = client.request("POST", URL, json=body)
        return response.get("id"), response.get("name"), "created"
    except Exception as ex:
        logger.error(
            "createParentSegment exception: %s, URL: %s, body: %s, response: %s",
            str(ex),
            URL,
            body,
            response if response else "No Response",
        )
        raise


def updateParentSegment(client, body, id):
    try:
        response = None
        URL = f"/audiences/{id}"
        response = client.request("PUT", URL, json=body)
        return response.get("id"), response.get("name"), "updated"
    except Exception as ex:
        logger.error(
            "updateParentSegment exception: %s, URL: %s, body: %s, response: %s",
            str(ex),
            URL,
            body,
            response if response else "No Response",
        )
        raise


def deleteParentSegment(client, id):
    try:
        response = None
        URL = f"/audiences/{id}"
        response = client.request("DELETE", URL)
        return response.get("id"), response.get("name"), "updated"
    except Exception as ex:
        logger.error(
            "deleteParentSegment exception: %s, URL: %s, response: %s",
            str(ex),
            URL,
            response if response else "No Response",
        )
        raise


def getParentSegment(client, _body):
    try:
        import json

        # Ensure _body is a dictionary
        if isinstance(_body, str):
            try:
                _body = json.loads(_body)
            except json.JSONDecodeError as e:
                logger.error("Failed to parse _body as JSON: %s", str(e))
                raise

        if not isinstance(_body, dict):
            raise TypeError("_body must be a dictionary or valid JSON string")

        response = None
        URL = f"/audiences"
        response = client.request("GET", URL)
        logger.info(
            "getParentSegment _body['id']: %s, _body['name']: %s",
            _body.get("id"),
            _body.get("name"),
        )
        for row in response:
            logger.info(
                "getParentSegment row['id']: %s, _body['id']: %s",
                row["id"],
                _body.get("id"),
            )
            if _body.get("id") and str(row["id"]) == str(_body["id"]):
                logger.info("Found Parent Segment by id %s", row.get("id"))
                return (row.get("id"), row.get("name"), "selected")
            elif (
                not _body.get("id")
                and _body.get("name")
                and str(row["name"]) == str(_body["name"])
            ):
                logger.info("Found Parent Segment by name %s", row.get("id"))
                return (row.get("id"), row.get("name"), "selected")
            elif not _body.get("id") and not _body.get("name"):
                logger.warning(
                    "At least name or id should be provided in the parent segment template yml file."
                )
                break
        # No Matches
        logger.info("Not Found Parent Segment %s", _body.get("id"))
        return None, None, "selected"
    except Exception as ex:
        logger.error(
            "getParentSegment exception: %s, URL: %s, response: %s",
            str(ex),
            URL,
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
            logger.info("ps_check_and_update update parent segment")
            audience_id, name, message = updateParentSegment(client, body, audience_id)
        elif not audience_id:
            logger.info("ps_check_and_update create parent segment")
            audience_id, name, message = createParentSegment(client, body)
        return audience_id, name, message
    except Exception as ex:
        logger.info(
            "ps_check_and_update exception for create and update now doing deletion: %s",
            str(ex),
        )
        _audience_id, _name, _message = deleteParentSegment(client, audience_id)
        logger.info("ps_check_and_update create parent segment after deletion")
        audience_id, name, message = createParentSegment(client, body)
        return audience_id, name, message
