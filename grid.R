# =============================================================================
# REQUIRED LIBRARIES
# =============================================================================


# Install packages
library(terra)      # Raster operations (SpatRaster)
library(sf)         # Vector operations (sf objects)
library(tigris)     # County boundaries
library(FedData)    # NLCD landcover data

# Set options
options(tigris_use_cache = TRUE)
options(timeout = 1800)


# =============================================================================
# COUNTY GRID INFRASTRUCTURE LAYER GENERATOR
# =============================================================================
# Creates 2-band TIFF (N-1 Contingency + Grid Connectivity) for utility-scale
# solar siting, using Voronoi nearest-neighbor assignment from substation points
# =============================================================================

get_county_grid_layer <- function(
  county_geoid,
  nodes_file = "grid/nodes_with_network_scores.csv",
  max_influence_distance = 25000,
  max_feasible_path_weight = 50,
  idw_power = 2,
  output_dir = "shapefiles/grid",
  write_tiff = TRUE
) {

  # =============================================================================
  # STEP 1: COUNTY SETUP & BUFFERING
  # =============================================================================

  cat("[GRID:STEP1] Loading county", county_geoid, "\n")

  counties_sf <- tigris::counties(cb = TRUE, resolution = "5m", year = 2023)
  county_poly <- counties_sf[counties_sf$GEOID == county_geoid, ]

  if (nrow(county_poly) == 0) stop("County not found: ", county_geoid)

  cat("[GRID:STEP1] County:", county_poly$NAME[1], "\n")

  county_proj <- st_transform(county_poly, crs = 5070)
  county_buffered <- st_buffer(county_proj, dist = 30000)

  cat("[GRID:STEP1] Buffered county by 30km\n")

  # =============================================================================
  # STEP 2: LOAD & FILTER SUBSTATIONS
  # =============================================================================

  cat("[GRID:STEP2] Loading substations from", nodes_file, "\n")

  nodes <- read.csv(nodes_file, stringsAsFactors = FALSE)

  cat("[GRID:STEP2] Total substations loaded:", nrow(nodes), "\n")

  nodes$shortest_path_weight <- ifelse(
    is.infinite(nodes$shortest_path_weight),
    max_feasible_path_weight * 2,
    nodes$shortest_path_weight
  )

  nodes$n1_contingency[is.na(nodes$n1_contingency)] <- 0
  nodes$shortest_path_weight[is.na(nodes$shortest_path_weight)] <- max_feasible_path_weight * 2

  nodes_sf <- st_as_sf(nodes, coords = c("long", "lat"), crs = 4326, remove = FALSE)
  nodes_proj <- st_transform(nodes_sf, crs = 5070)
  nodes_in_area <- nodes_proj[county_buffered, ]

  cat("[GRID:STEP2] Substations in buffered area:", nrow(nodes_in_area), "\n")

  if (nrow(nodes_in_area) == 0) {
    warning("No substations found within 30km of county ", county_geoid)
    return(NULL)
  }

  # =============================================================================
  # STEP 3: CREATE TEMPLATE RASTER
  # =============================================================================

  cat("[GRID:STEP3] Loading NLCD as template\n")

  nlcd <- get_nlcd_landcover_spat(
    county_poly,
    year = 2019,
    label = paste0("County_", county_geoid),
    county_geoid = county_geoid
  )

  if (is.null(nlcd)) stop("Failed to load NLCD template for county ", county_geoid)

  cat("[GRID:STEP3] NLCD template loaded\n")

  nodes_nlcd_crs <- st_transform(nodes_in_area, terra::crs(nlcd))

  cat("[GRID:STEP3] Transformed", nrow(nodes_nlcd_crs), "substations to NLCD CRS\n")

  # =============================================================================
  # STEP 4: COMPUTE VORONOI ZONES
  # =============================================================================

  cat("[GRID:STEP4] Computing Voronoi zones\n")

  template <- nlcd * 0

  nodes_nlcd_crs$substation_id <- 1:nrow(nodes_nlcd_crs)
  nodes_vect <- terra::vect(nodes_nlcd_crs)

  voro <- terra::voronoi(nodes_vect, ext(template))

  # Transfer attributes from points to voronoi polygons
  voro$substation_id <- nodes_nlcd_crs$substation_id

  cat("[GRID:STEP4] Rasterizing Voronoi zones\n")

  nearest_idx <- terra::rasterize(voro, template, field = "substation_id")

  cat("[GRID:STEP4] Computing distances to nearest substation\n")

  dist_to_nearest <- terra::distance(nearest_idx)

  # =============================================================================
  # STEP 5: CREATE BANDS FROM NEAREST SUBSTATION SCORES
  # =============================================================================

  cat("[GRID:STEP5] Assigning scores to pixels\n")

  n1_scores <- nodes_nlcd_crs$n1_contingency
  path_weights <- nodes_nlcd_crs$shortest_path_weight
  connectivity_scores <- 100 * (1 - pmin(path_weights / max_feasible_path_weight, 1))

  band1_n1 <- template
  band2_connectivity <- template

  terra::values(band1_n1) <- n1_scores[terra::values(nearest_idx)]
  terra::values(band2_connectivity) <- connectivity_scores[terra::values(nearest_idx)]

  cat("[GRID:STEP5] Applying 25km distance threshold\n")

  band1_n1[dist_to_nearest > max_influence_distance] <- NA
  band2_connectivity[dist_to_nearest > max_influence_distance] <- NA

  cat("[GRID:STEP5] Stacking bands\n")

  grid_raster <- c(band1_n1, band2_connectivity)
  names(grid_raster) <- c("n1_contingency", "grid_connectivity")

  county_nlcd_crs <- st_transform(county_poly, terra::crs(nlcd))
  grid_raster_masked <- terra::mask(grid_raster, terra::vect(county_nlcd_crs))

  cat("[GRID:STEP5] Complete\n")

  # =============================================================================
  # STEP 6: WRITE TIFF OUTPUT
  # =============================================================================

  if (write_tiff) {
    out_dir <- file.path(output_dir, county_geoid)
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
    out_file <- file.path(out_dir, paste0("grid_layer_", county_geoid, ".tif"))
    terra::writeRaster(grid_raster_masked, out_file, overwrite = TRUE)
    cat("[GRID:STEP6] Saved TIFF:", out_file, "\n")
  }

  # =============================================================================
  # STEP 7: COMPUTE STATISTICS
  # =============================================================================

  band1_vals <- terra::values(grid_raster_masked[[1]], na.rm = TRUE)
  band2_vals <- terra::values(grid_raster_masked[[2]], na.rm = TRUE)

  total_pixels <- terra::ncell(grid_raster_masked[[1]])
  valid_pixels <- sum(!is.na(band1_vals))
  na_pixels <- sum(is.na(terra::values(grid_raster_masked[[1]])))

  high_quality_pixels <- sum(band1_vals >= 75 & band2_vals >= 75, na.rm = TRUE)

  stats <- data.frame(
    county_geoid = county_geoid,
    county_name = county_poly$NAME[1],
    substations = nrow(nodes_nlcd_crs),
    total_pixels = total_pixels,
    valid_pixels = valid_pixels,
    na_pixels = na_pixels,
    coverage_pct = round((valid_pixels / total_pixels) * 100, 1),
    mean_n1 = round(mean(band1_vals, na.rm = TRUE), 2),
    mean_connectivity = round(mean(band2_vals, na.rm = TRUE), 2),
    high_quality_pixels = high_quality_pixels,
    high_quality_pct = round((high_quality_pixels / valid_pixels) * 100, 1)
  )

  cat("\n[STATS]\n")
  print(stats)

  # =============================================================================
  # RETURN
  # =============================================================================

  return(list(
    grid_raster = grid_raster_masked,
    substations_used = nodes_nlcd_crs,
    county_poly = county_poly,
    stats = stats
  ))
}
