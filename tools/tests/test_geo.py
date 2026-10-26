import h3

from radar_tools import geo


def test_densify_keeps_ends_and_spacing():
    line = [(59.35, 24.05), (60.04, 24.00)]  # ~77 km
    pts = geo.densify(line, step_km=1.0)
    assert pts[0] == line[0] and pts[-1] == line[-1]
    gaps = [geo._haversine_km(a, b) for a, b in zip(pts, pts[1:], strict=False)]
    assert max(gaps) <= 1.0


def test_cable_zone_covers_the_route_and_its_sides():
    cells = geo.cable_cells()
    # Mitad del Balticconnector y un punto a ~1,5 km al este del trazado.
    assert "Balticconnector" in cells[h3.latlng_to_cell(59.70, 24.02, geo.CABLE_RES)]
    assert h3.latlng_to_cell(59.70, 24.047, geo.CABLE_RES) in cells
    # Lejos de cualquier cable (centro del Skagerrak).
    assert h3.latlng_to_cell(58.0, 9.5, geo.CABLE_RES) not in cells


def test_port_zone():
    cells = geo.port_cells()
    assert cells[h3.latlng_to_cell(51.95, 4.05, geo.PORT_RES)] == "Rotterdam"
    assert h3.latlng_to_cell(56.0, 3.0, geo.PORT_RES) not in cells  # mar abierto
