# =============================================================================
# COUNTY COMPARISON AND OVERLAY ANALYSIS
# =============================================================================

library(terra)
library(sf)
library(tigris)

options(tigris_use_cache = TRUE)

# =============================================================================
# FUNCTION 1: COMPARE COUNTY SUITABILITY
# =============================================================================

compare_county_suitability <- function(county_geoids, output_file = "county_comparison.csv") {

  cat("[COMPARE] Starting multi-county comparison\n")
  cat("[COMPARE] Counties to process:", length(county_geoids), "\n")

  # Initialize results storage
  results <- data.frame()

  # =============================================================================
  # STEP 1: LOOP THROUGH COUNTIES
  # =============================================================================

  for (i in seq_along(county_geoids)) {
    geoid <- county_geoids[i]
    cat("\n[COMPARE:", i, "/", length(county_geoids), "] Processing county", geoid, "\n")

    # -------------------------------------------------------------------------
    # STEP 1A: CHECK FILES EXIST
    # -------------------------------------------------------------------------

    land_shp <- file.path("shapefiles/land", geoid, paste0("largest_patch_", geoid, ".shp"))
    grid_tif <- file.path("shapefiles/grid", geoid, paste0("grid_layer_", geoid, ".tif"))

    cat("[CHECK] Land shapefile:", land_shp, "\n")
    cat("[CHECK] Land file exists:", file.exists(land_shp), "\n")

    cat("[CHECK] Grid TIFF:", grid_tif, "\n")
    cat("[CHECK] Grid file exists:", file.exists(grid_tif), "\n")

    if (!file.exists(land_shp)) {
      cat("[SKIP] Land shapefile not found for county", geoid, "\n")
      next
    }

    if (!file.exists(grid_tif)) {
      cat("[SKIP] Grid TIFF not found for county", geoid, "\n")
      next
    }

    # -------------------------------------------------------------------------
    # STEP 1B: READ FILES
    # -------------------------------------------------------------------------

    cat("[READ] Loading land shapefile\n")
    land_poly <- tryCatch({
      sf::st_read(land_shp, quiet = TRUE)
    }, error = function(e) {
      cat("[ERROR] Failed to read land shapefile:", as.character(e), "\n")
      return(NULL)
    })

    if (is.null(land_poly)) {
      cat("[SKIP] Could not read land file for county", geoid, "\n")
      next
    }

    cat("[READ] Land polygon loaded:", nrow(land_poly), "features\n")

    cat("[READ] Loading grid TIFF\n")
    grid_raster <- tryCatch({
      terra::rast(grid_tif)
    }, error = function(e) {
      cat("[ERROR] Failed to read grid TIFF:", as.character(e), "\n")
      return(NULL)
    })

    if (is.null(grid_raster)) {
      cat("[SKIP] Could not read grid file for county", geoid, "\n")
      next
    }

    cat("[READ] Grid raster loaded:", terra::nlyr(grid_raster), "bands\n")
    cat("[READ] Band names:", paste(names(grid_raster), collapse = ", "), "\n")

    # -------------------------------------------------------------------------
    # STEP 1C: CALCULATE TOTAL BUILDABLE ACRES
    # -------------------------------------------------------------------------

    cat("[CALC] Computing buildable area\n")

    # Get area in square meters, convert to acres
    land_poly_proj <- sf::st_transform(land_poly, sf::st_crs(grid_raster))
    area_m2 <- as.numeric(sf::st_area(land_poly_proj))
    buildable_acres <- area_m2 * 0.000247105

    cat("[CALC] Total buildable acres:", round(buildable_acres, 2), "\n")

    # -------------------------------------------------------------------------
    # STEP 1D: RASTERIZE LAND POLYGON TO MATCH GRID
    # -------------------------------------------------------------------------

    cat("[RASTERIZE] Converting land polygon to raster\n")

    land_raster <- terra::rasterize(
      terra::vect(land_poly_proj),
      grid_raster[[1]],
      field = 1,
      background = 0
    )

    land_mask <- land_raster == 1

    buildable_pixels <- sum(terra::values(land_mask), na.rm = TRUE)
    cat("[RASTERIZE] Buildable pixels:", buildable_pixels, "\n")

    # -------------------------------------------------------------------------
    # STEP 1E: EXTRACT GRID SCORES ONLY WHERE LAND IS BUILDABLE
    # -------------------------------------------------------------------------

    cat("[EXTRACT] Masking grid scores to buildable land\n")

    n1_band <- grid_raster[[1]]
    conn_band <- grid_raster[[2]]

    # Apply land mask - only keep grid scores where land is buildable
    n1_buildable <- terra::mask(n1_band, land_mask, maskvalues = c(0, NA))
    conn_buildable <- terra::mask(conn_band, land_mask, maskvalues = c(0, NA))

    n1_vals <- terra::values(n1_buildable, na.rm = TRUE)
    conn_vals <- terra::values(conn_buildable, na.rm = TRUE)

    cat("[EXTRACT] N-1 values extracted:", length(n1_vals), "\n")
    cat("[EXTRACT] Connectivity values extracted:", length(conn_vals), "\n")

    if (length(n1_vals) == 0 || length(conn_vals) == 0) {
      cat("[SKIP] No valid grid scores in buildable area for county", geoid, "\n")
      next
    }

    # -------------------------------------------------------------------------
    # STEP 1F: CALCULATE MEAN SCORES
    # -------------------------------------------------------------------------

    cat("[METRICS] Computing mean scores across buildable land\n")

    mean_n1 <- mean(n1_vals, na.rm = TRUE)
    mean_conn <- mean(conn_vals, na.rm = TRUE)

    cat("[METRICS] Mean N-1:", round(mean_n1, 2), "\n")
    cat("[METRICS] Mean connectivity:", round(mean_conn, 2), "\n")

    # -------------------------------------------------------------------------
    # STEP 1G: GET COUNTY NAME
    # -------------------------------------------------------------------------

    cat("[INFO] Fetching county name\n")
    county_name <- tryCatch({
      counties_sf <- tigris::counties(cb = TRUE, resolution = "5m", year = 2023)
      county_row <- counties_sf[counties_sf$GEOID == geoid, ]
      if (nrow(county_row) > 0) {
        paste0(county_row$NAME[1], ", ", county_row$STUSPS[1])
      } else {
        paste0("County_", geoid)
      }
    }, error = function(e) {
      paste0("County_", geoid)
    })

    cat("[INFO] County name:", county_name, "\n")

    # -------------------------------------------------------------------------
    # STEP 1H: STORE RESULTS
    # -------------------------------------------------------------------------

    results <- rbind(results, data.frame(
      county_geoid = geoid,
      county_name = county_name,
      buildable_acres = round(buildable_acres, 2),
      mean_n1 = round(mean_n1, 2),
      mean_connectivity = round(mean_conn, 2),
      stringsAsFactors = FALSE
    ))

    cat("[STORED] Results stored for county", geoid, "\n")
  }

  # =============================================================================
  # STEP 2: SORT BY BUILDABLE ACRES (DESCENDING)
  # =============================================================================

  cat("\n[SORT] Sorting results by buildable acres\n")

  if (nrow(results) == 0) {
    cat("[ERROR] No valid results to sort\n")
    return(NULL)
  }

  results <- results[order(results$buildable_acres, decreasing = TRUE), ]

  cat("[SORT] Sorted", nrow(results), "counties\n")

  # =============================================================================
  # STEP 3: WRITE OUTPUT
  # =============================================================================

  if (!is.null(output_file)) {
    cat("[OUTPUT] Writing results to", output_file, "\n")
    write.csv(results, output_file, row.names = FALSE)
    cat("[OUTPUT] File written successfully\n")
  }

  # =============================================================================
  # STEP 4: PRINT SUMMARY
  # =============================================================================

  cat("\n")
  cat("================================================================================\n")
  cat("COUNTY COMPARISON SUMMARY\n")
  cat("================================================================================\n\n")
  print(results)

  cat("\n[COMPARE] Analysis complete\n")

  return(results)
}
