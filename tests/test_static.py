import io
import zipfile

from tracker.static_gtfs import parse_routes


def make_zip(routes_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("routes.txt", routes_text.encode("utf-8-sig"))
    return buffer.getvalue()


def test_reads_routes_with_bom_and_blank_names():
    text = (
        "route_id,agency_id,route_short_name,route_long_name,route_type\n"
        '6612,TL,099,"Commercial-Broadway/UBC",3\n'
        "30055,TL,,Expo Line,1\n"
    )
    routes = parse_routes(make_zip(text))
    assert routes[0] == {
        "route_id": "6612", "short_name": "099",
        "long_name": "Commercial-Broadway/UBC", "route_type": 3,
    }
    assert routes[1]["short_name"] is None
    assert routes[1]["route_type"] == 1
