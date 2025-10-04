# =============================================================================
# LAND SUITABILITY
# =============================================================================
# - Downloads NLCD LandCover for a county AOI (robust loader, persistent cache)
# - Optionally downloads state wetlands and subsets to county via GDAL wkt_filter
# - Fetches PAD-US (USGS protected areas) for county and excludes GAP 1/2 lands
# - Computes slope from DEM and excludes areas above a threshold
# - Uses FEMA NFIP claims to create flood exclusion buffers
# - Parameterizes wetlands buffer for sensitivity analysis (e.g., 50/100/200 m)
# - Logs per-exclusion impact (delta pixels and percent change) at each step
# - Caches large datasets persistently:
#     * NLCD outputs under ./cache/nlcd
#     * State wetlands GPKGs under ./cache/wetlands
# - Writes the largest contiguous patch as a shapefile by default (like OG code)
# =============================================================================

# =============================================================================
# INSTALLATION (RUN ONLY IF PACKAGES ARE MISSING)
# =============================================================================
# install.packages(c("FedData", "tigris", "terra", "sf", "elevatr", "curl"))
# install.packages(c("plyr", "memoise"))  # rfema dependencies
# install.packages("rfema", repos = "https://ropensci.r-universe.dev")

# =============================================================================
# LOAD LIBRARIES
# =============================================================================
library(FedData)   # NLCD & PAD-US helpers
library(tigris)    # County & state geographies
library(terra)     # Raster operations (SpatRaster)
library(sf)        # Vector operations (sf)
library(elevatr)   # DEM mosaicking
library(rfema)     # FEMA NFIP claims API
library(curl)      # Robust downloads (wetlands zip)

options(tigris_use_cache = TRUE)   # Cache TIGRIS shapefiles locally
options(timeout = 1800)            # Generous timeout for web requests

# =============================================================================
# PERSISTENT CACHE DIRECTORIES
# =============================================================================
# Use persistent cache folders to avoid repeated downloads across sessions
CACHE_DIR <- file.path(getwd(), "cache")
NLCD_CACHE_DIR <- file.path(CACHE_DIR, "nlcd")
WETLANDS_CACHE_DIR <- file.path(CACHE_DIR, "wetlands")
dir.create(CACHE_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(NLCD_CACHE_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(WETLANDS_CACHE_DIR, showWarnings = FALSE, recursive = TRUE)

# =============================================================================
# STATE DATA AND MAPPINGS (for printing and FEMA state codes)
# =============================================================================
cat("[INIT] Loading states and building state mappings...\n")
states_data <- states(cb = TRUE, year = 2023)
state_code_map <- setNames(states_data$STUSPS, states_data$STATEFP)  # FIPS -> USPS
state_name_map <- setNames(states_data$NAME, states_data$STATEFP)    # FIPS -> Name

# =============================================================================
# FEMA HIGH-RISK FLOOD ZONES
# =============================================================================
# These indicate significant flood hazard; used to filter NFIP claims
high_risk_zones <- c(
  "A", paste0("A", 1:30), "AE", "AH", "AO", "A99",
  "V", paste0("V", 1:30), "VE",
  "AR", "AR/A", "AR/AE", "AR/AH",
  "100", "IN", "FW"
)

# =============================================================================
# DEBUG LOGGING HELPER
# =============================================================================
debug_log <- function(step, ...) {
  cat(paste0("[", step, "] "), paste0(..., collapse = " "), "\n")
}

# =============================================================================
# UTILITY: Logical FALSE mask matching a template SpatRaster (preserve NA)
# =============================================================================
false_mask_like <- function(template) {
  (template * 0) == 1  # FALSE where template is not NA; NA preserved
}

# =============================================================================
# Helper: find a county GEOID by state abbreviation and county name
# =============================================================================
get_geoid <- function(state_abbrev, county_name, year = 2023) {
  st <- tigris::states(cb = TRUE, year = year)
  st_row <- st[st$STUSPS == state_abbrev, ]
  if (nrow(st_row) == 0) stop("State not found: ", state_abbrev)
  co <- tigris::counties(cb = TRUE, resolution = "5m", year = year)
  co_st <- co[co$STATEFP == st_row$STATEFP, ]
  co_row <- co_st[trimws(tolower(co_st$NAME)) == trimws(tolower(county_name)), ]
  if (nrow(co_row) == 0) stop("County not found: ", county_name, " in ", state_abbrev)
  co_row$GEOID[1]
}

# =============================================================================
# ROBUST NLCD LOADER (LANDCOVER) WITH PERSISTENT CACHE
# =============================================================================
# Adapts to FedData::get_nlcd signatures ("aoi" vs "template"), supports retry/backoff,
# and falls back to terra::values when global/minmax are unreliable for categorical rasters.
get_nlcd_landcover_spat <- function(aoi_sf, year, label, county_geoid,
                                    redo = FALSE, retries = 3, fallback_year = 2016) {
  step <- paste0("NLCD:", label)
  debug_log(step, "Requesting NLCD landcover for year", year, " redo:", redo)

  # Persistent extraction directory per county/year to avoid stale cache issues
  extraction_dir <- file.path(NLCD_CACHE_DIR, paste0("County_", county_geoid, "_", year, if (redo) "_redo" else ""))
  dir.create(extraction_dir, showWarnings = FALSE, recursive = TRUE)

  # Determine get_nlcd signature
  gn <- FedData::get_nlcd
  arg_names <- try(names(formals(gn)), silent = TRUE)
  if (inherits(arg_names, "try-error")) arg_names <- character()

  # Build argument list for get_nlcd calls
  mk_args <- function(y, rdo) {
    args <- list(
      year = y,
      dataset = "landcover",  # must be lowercase
      label = label,
      force.redo = rdo
    )
    if ("extraction.dir" %in% arg_names) args$extraction.dir <- extraction_dir
    if ("aoi" %in% arg_names) {
      args$aoi <- aoi_sf
      debug_log(step, "Using 'aoi=' argument; extraction.dir:", extraction_dir)
    } else if ("template" %in% arg_names) {
      args$template <- aoi_sf
      debug_log(step, "Using 'template=' argument; extraction.dir:", extraction_dir)
    } else {
      args <- c(list(aoi_sf), args) # positional fallback
      debug_log(step, "Unknown signature; using positional AOI; extraction.dir:", extraction_dir)
    }
    args
  }

  # Try fetch with retries
  attempt <- function(y, rdo) {
    tryCatch({
      do.call(gn, mk_args(y, rdo))
    }, error = function(e) {
      debug_log(step, "ERROR get_nlcd(year=", y, ", redo=", rdo, "):", as.character(e))
      NULL
    })
  }

  rl <- NULL
  for (i in seq_len(retries + 1)) {
    rl <- attempt(year, redo)
    if (!is.null(rl)) break
    if (i <= retries) {
      debug_log(step, "Retry", i, "in 3s ...")
      Sys.sleep(3)
    }
  }

  # Fallback year if primary failed
  if (is.null(rl) && !redo) {
    debug_log(step, "Primary year failed; trying fallback year", fallback_year)
    rl <- attempt(fallback_year, TRUE)
  }

  # Correction 3: Return NULL on failure (explicit signal)
  if (is.null(rl)) {
    debug_log(step, "NLCD completely unavailable")
    return(NULL)
  }

  # Normalize to terra SpatRaster and select LandCover layer
  sr <- if (inherits(rl, "SpatRaster")) rl else terra::rast(rl)
  nm <- tolower(names(sr))
  lc_idx <- grep("landcover", nm)
  if (length(lc_idx) == 0) lc_idx <- 1
  sr <- sr[[lc_idx[1]]]

  # Validate values (minmax/global may be unreliable for categorical rasters)
  mm <- try(terra::minmax(sr), silent = TRUE)
  non_na <- try(terra::global(sr, fun = "count", na.rm = TRUE)[1, 1], silent = TRUE)
  debug_log(step, "CRS:", terra::crs(sr), "Res:", paste(terra::res(sr), collapse = ","), "ncell:", terra::ncell(sr))
  debug_log(step, "minmax:", if (!inherits(mm, "try-error")) paste(mm[1, ], collapse = ",") else "ERR",
            "non-NA:", if (!inherits(non_na, "try-error")) non_na else "ERR")

  # Fallback: explicitly read values
  if (inherits(non_na, "try-error") || is.na(non_na) || (!inherits(non_na, "try-error") && non_na == 0)) {
    vals <- try(terra::values(sr), silent = TRUE)
    if (!inherits(vals, "try-error") && length(vals) > 0) {
      non_na2 <- sum(!is.na(vals))
      debug_log(step, "Fallback values() length:", length(vals), "non-NA:", non_na2)
      if (non_na2 == 0) {
        debug_log(step, "Values unreadable; returning NULL")
        return(NULL)
      }
    } else {
      debug_log(step, "values() read failed; returning NULL")
      return(NULL)
    }
  }

  sr
}

# =============================================================================
# WETLANDS: DOWNLOAD STATE GPKG (CACHED) AND READ COUNTY SUBSET
# =============================================================================
# =============================================================================
# CORRECTED WETLANDS LOADER WITH STATE-SPECIFIC CACHE DIRECTORIES
# =============================================================================

get_state_wetlands <- function(state_code, timeout = 1800) {
  step <- paste0("WETLANDS:", state_code)
  debug_log(step, "Starting wetlands loader")

  # Create state-specific cache subdirectory
  state_cache_dir <- file.path(WETLANDS_CACHE_DIR, state_code)
  dir.create(state_cache_dir, showWarnings = FALSE, recursive = TRUE)

  base_url <- "https://documentst.ecosphere.fws.gov/wetlands/data/State-Downloads/"
  zip_url <- paste0(base_url, state_code, "_geopackage_wetlands.zip")
  zip_file <- file.path(state_cache_dir, paste0(state_code, "_wetlands.zip"))
  gpkg_file <- NULL

  # Reuse existing GPKG if present in state-specific cache
  candidates <- list.files(state_cache_dir, pattern = "\\.gpkg$",
                           ignore.case = TRUE, full.names = TRUE)
  if (length(candidates) > 0) {
    gpkg_file <- candidates[1]
    debug_log(step, "Using existing GPKG:", gpkg_file)
  }

  if (is.null(gpkg_file)) {
    debug_log(step, "Downloading zip (may be very large):", zip_url)
    h <- curl::new_handle(timeout = timeout, connecttimeout = 60, low_speed_time = 60, low_speed_limit = 1024)
    ok <- tryCatch({
      curl::curl_download(zip_url, zip_file, mode = "wb", handle = h)
      unzip(zip_file, exdir = state_cache_dir)
      TRUE
    }, error = function(e) {
      debug_log(step, "ERROR downloading:", as.character(e))
      FALSE
    })
    if (!ok) {
      debug_log(step, "Returning NULL (download failed)")
      return(NULL)
    }

    # Search only in state-specific directory
    gpkg_list <- list.files(state_cache_dir, pattern = "\\.gpkg$", ignore.case = TRUE, full.names = TRUE)
    if (length(gpkg_list) == 0) {
      debug_log(step, "No GPKG found after unzip")
      return(NULL)
    }
    wetlands_gpkg <- grep("Wetlands", gpkg_list, value = TRUE, ignore.case = TRUE)
    gpkg_file <- if (length(wetlands_gpkg) > 0) wetlands_gpkg[1] else gpkg_list[1]
    debug_log(step, "Selected GPKG:", gpkg_file)
    if (file.exists(zip_file)) file.remove(zip_file)
  }

  layers <- st_layers(gpkg_file)
  debug_log(step, "GPKG layers:", paste(layers$name, collapse = ", "))
  target <- paste0("^", state_code, "_Wetlands$")
  wetlands_layer <- NULL
  for (nm in layers$name) {
    if (grepl(target, nm) && !grepl("Historic|Metadata|Project|Map_Info", nm, ignore.case = TRUE)) {
      wetlands_layer <- nm
      break
    }
  }
  if (is.null(wetlands_layer)) {
    for (nm in layers$name) {
      if (grepl("Wetlands", nm, ignore.case = TRUE) &&
          !grepl("Historic|Metadata|Project|Map_Info", nm, ignore.case = TRUE)) {
        wetlands_layer <- nm
        break
      }
    }
  }
  if (is.null(wetlands_layer)) {
    debug_log(step, "No wetlands layer found; returning NULL")
    return(NULL)
  }
  debug_log(step, "Wetlands layer:", wetlands_layer)
  list(gpkg = gpkg_file, layer = wetlands_layer)
}


read_county_wetlands <- function(wetlands_ref, county_poly) {
  if (is.null(wetlands_ref)) return(NULL)
  debug_log("WETLANDS:READ", "Determining layer CRS with LIMIT 1 query")
  q <- paste0("SELECT * FROM ", wetlands_ref$layer, " LIMIT 1")
  one <- tryCatch(st_read(wetlands_ref$gpkg, query = q, quiet = TRUE), error = function(e) NULL)
  if (is.null(one)) {
    debug_log("WETLANDS:READ", "Failed to read one feature; fallback to full layer (may be heavy)")
    return(tryCatch(st_read(wetlands_ref$gpkg, layer = wetlands_ref$layer, quiet = TRUE),
                    error = function(e) NULL))
  }
  target_crs <- st_crs(one)
  county_geom <- st_geometry(st_transform(county_poly, target_crs))
  wkt <- st_as_text(county_geom)
  debug_log("WETLANDS:READ", "Reading layer with wkt_filter")
  tryCatch(st_read(wetlands_ref$gpkg, layer = wetlands_ref$layer, wkt_filter = wkt, quiet = TRUE),
           error = function(e) NULL)
}

# =============================================================================
# Function: get_county_protected_areas
# Purpose:
#   Retrieve USGS PAD-US (Protected Areas Database of the U.S.) geometries that
#   intersect a county AOI. Normalize the output to sf, detect the GAP status
#   field name, and report feature counts for downstream exclusion decisions.
#
# Background on GAP Status Codes (PAD-US):
#   - GAP 1: Permanent protection for biodiversity with a strict mandate;
#            Examples: Wilderness Areas, National Parks. Development incompatible.
#   - GAP 2: Permanent protection with mandated management for biodiversity;
#            Examples: National Wildlife Refuges, National Monuments. Development incompatible.
#   - GAP 3: Multiple-use management with some protection from conversion;
#            Examples: National Forests, BLM lands. Development may be debated (permits/siting).
#   - GAP 4: No known biodiversity protection mandate;
#            Examples: Private lands, agricultural areas. No inherent protection-based restrictions.
#
# Notes:
#   - FedData::get_padus sometimes returns different structures across versions:
#       * a single sf object, or
#       * a list of layers, or
#       * an sp/Spatial object.
#     This function normalizes to sf and returns the full dataset for later filtering.
#   - The GAP status field can be named "GAP_Sts" or "GAP_Status" depending on the data version.
#   - Exclusion policy (e.g., removing GAP 1/2) is applied downstream (in your main function),
#     so this function remains a clean data fetcher.
#
# Inputs:
#   county_geoid: character, 5-digit county GEOID (e.g., "17031" for Cook County, IL)
#
# Outputs:
#   sf object of PAD-US features intersecting the county. Returns NULL on failure.
# =============================================================================
get_county_protected_areas <- function(county_geoid) {
  step <- paste0("PADUS:", county_geoid)
  debug_log(step, "Fetching county polygon and PAD-US ...")

  # 1) Build county AOI from TIGRIS; fail fast if GEOID not found.
  counties_sf <- tigris::counties(cb = TRUE, resolution = "5m", year = 2023)
  county_poly <- counties_sf[counties_sf$GEOID == county_geoid, ]
  if (nrow(county_poly) == 0) {
    debug_log(step, "ERROR: County not found")
    return(NULL)
  }

  # 2) Call FedData::get_padus; wrap in tryCatch to avoid halting on download/service errors.
  padus <- NULL
  ok <- tryCatch({
    padus <- FedData::get_padus(county_poly, label = paste0("padus_", county_geoid))
    TRUE
  }, error = function(e) {
    debug_log(step, "ERROR get_padus:", as.character(e))
    FALSE
  })
  if (!ok || is.null(padus)) {
    # No data returned; propagate a NULL result for downstream handling.
    return(NULL)
  }

  # 3) Normalize to sf:
  #    - If a list, extract the first sf element.
  #    - If sp/Spatial, convert to sf.
  if (is.list(padus) && !"sf" %in% class(padus)) {
    for (item in padus) {
      if (inherits(item, "sf")) {
        padus <- item
        break
      }
    }
  }
  if (inherits(padus, "Spatial")) {
    padus <- sf::st_as_sf(padus)
  }
  if (!inherits(padus, "sf")) {
    debug_log(step, "No sf geometry; returning NULL")
    return(NULL)
  }

  # 4) Detect GAP status field name. PAD-US schema may use GAP_Sts or GAP_Status.
  gap_field <- if ("GAP_Sts" %in% names(padus)) {
    "GAP_Sts"
  } else if ("GAP_Status" %in% names(padus)) {
    "GAP_Status"
  } else {
    NA
  }

  # 5) Log a summary: feature count and which GAP field was detected.
  debug_log(
    step,
    "Features:", nrow(padus),
    "GAP field:", ifelse(is.na(gap_field), "missing", gap_field)
  )

  # Return the full sf dataset. Downstream code will:
  #   - pick GAP codes to exclude (typically c("1","2")),
  #   - transform CRS to match raster,
  #   - rasterize and mask.
  padus
}

# =============================================================================
# EXCLUSION FUNCTIONS (WITH VALIDATION AND CORRECTIONS)
# =============================================================================

# Wetlands exclusion: buffer county wetlands and rasterize to logical mask
add_wetlands_exclusion <- function(county_geoid, buildable_mask, county_wetlands, buffer_distance = 100) {
  step <- paste0("EXCL:WET:", county_geoid)

  # Correction 4: Validate buffer distance (10–1000m reasonable range)
  validated <- max(10, min(1000, buffer_distance))
  if (validated != buffer_distance) debug_log(step, "Input buffer clipped to", validated, "m")
  buffer_distance <- validated
  debug_log(step, "Starting wetlands exclusion with validated buffer:", buffer_distance, "m")

  if (is.null(county_wetlands) || nrow(county_wetlands) == 0) {
    debug_log(step, "No wetlands found; returning empty exclusion")
    return(false_mask_like(buildable_mask))
  }
  county_wetlands_proj <- st_transform(county_wetlands, terra::crs(buildable_mask))
  debug_log(step, "Buffering", nrow(county_wetlands_proj), "features by", buffer_distance, "m")
  wetlands_buffered <- st_buffer(county_wetlands_proj, dist = buffer_distance)
  debug_log(step, "Rasterizing buffered wetlands")
  wetlands_raster <- terra::rasterize(terra::vect(wetlands_buffered), buildable_mask,
                                      field = 1, background = 0)
  wetlands_exclusion <- !is.na(wetlands_raster) & (wetlands_raster == 1)
  excl_count <- sum(terra::values(wetlands_exclusion), na.rm = TRUE)
  debug_log(step, "Exclusion TRUE cells:", excl_count)
  wetlands_exclusion
}

# Slope exclusion: compute slope after projecting DEM to buildable CRS
add_slope_exclusion <- function(county_geoid, buildable_mask, slope_threshold = 8) {
  step <- paste0("EXCL:SLOPE:", county_geoid)

  # Correction 4: Validate slope threshold (0–45 degrees reasonable)
  validated <- max(0, min(45, slope_threshold))
  if (validated != slope_threshold) debug_log(step, "Input slope threshold clipped to", validated, "deg")
  slope_threshold <- validated
  debug_log(step, "Getting DEM and computing slope >", slope_threshold, "deg")

  counties_sf <- tigris::counties(cb = TRUE, resolution = "5m", year = 2023)
  county_poly <- counties_sf[counties_sf$GEOID == county_geoid, ]
  if (nrow(county_poly) == 0) {
    debug_log(step, "County not found; empty exclusion")
    return(false_mask_like(buildable_mask))
  }

  # Correction 2: Use z = 11 (~30m) to match NLCD resolution
  dem <- elevatr::get_elev_raster(county_poly, z = 11)
  dem_terra <- terra::rast(dem)
  dem_proj <- terra::project(dem_terra, buildable_mask)
  slope_raster <- terra::terrain(dem_proj, "slope", unit = "degrees")

  # Use bilinear resampling for continuous data
  slope_resampled <- terra::resample(slope_raster, buildable_mask, method = "bilinear")
  slope_exclusion <- slope_resampled > slope_threshold
  excluded_pixels <- sum(terra::values(slope_exclusion), na.rm = TRUE)
  total_pixels <- sum(!is.na(terra::values(slope_exclusion)))
  exclusion_percentage <- if (total_pixels > 0) (excluded_pixels / total_pixels) * 100 else 0
  debug_log(step, "Excluded:", excluded_pixels, "/", total_pixels, "(", round(exclusion_percentage, 1), "%)")
  slope_exclusion
}

# FEMA flood exclusion: buffer high-risk claims in projected CRS and rasterize
add_flood_exclusion_fema <- function(county_geoid, buildable_mask, buffer_distance = 300) {
  step <- paste0("EXCL:FEMA:", county_geoid)

  # Correction 4: Validate flood buffer (50–1000m reasonable)
  validated <- max(50, min(1000, buffer_distance))
  if (validated != buffer_distance) debug_log(step, "Input flood buffer clipped to", validated, "m")
  buffer_distance <- validated
  debug_log(step, "Creating FEMA exclusion with validated buffer:", buffer_distance, "m")

  state_fips <- substr(county_geoid, 1, 2)
  state_code <- state_code_map[state_fips]
  if (is.na(state_code)) {
    debug_log(step, "Unsupported state FIPS; empty exclusion")
    return(false_mask_like(buildable_mask))
  }
  counties_sf <- tigris::counties(cb = TRUE, resolution = "5m", year = 2023)
  county_poly <- counties_sf[counties_sf$GEOID == county_geoid, ]
  county_bbox <- sf::st_bbox(sf::st_transform(county_poly, 4326))

  res <- tryCatch({
    # Correction 1: No top_n limit - get ALL claims for complete coverage
    debug_log(step, "Fetching ALL FEMA claims for state", state_code, "(may be slow)")
    rfema::open_fema("FimaNfipClaims", ask_before_call = FALSE, filters = list(state = state_code))
  }, error = function(e) {
    debug_log(step, "ERROR open_fema:", as.character(e))
    NULL
  })

  if (is.null(res) || nrow(res) == 0) {
    debug_log(step, "No state claims; empty exclusion")
    return(false_mask_like(buildable_mask))
  }

  debug_log(step, "Total state claims retrieved:", nrow(res))

  res$lat_num <- suppressWarnings(as.numeric(res$latitude))
  res$lon_num <- suppressWarnings(as.numeric(res$longitude))
  in_bbox <- !is.na(res$lat_num) & !is.na(res$lon_num) &
    res$lat_num >= county_bbox[2] & res$lat_num <= county_bbox[4] &
    res$lon_num >= county_bbox[1] & res$lon_num <= county_bbox[3]
  county_claims <- res[in_bbox, ]
  debug_log(step, "Claims in county bbox:", nrow(county_claims))
  if (nrow(county_claims) == 0) return(false_mask_like(buildable_mask))

  high_risk_claims <- county_claims[county_claims$ratedFloodZone %in% high_risk_zones, ]
  debug_log(step, "High-risk claims:", nrow(high_risk_claims))
  if (nrow(high_risk_claims) == 0) return(false_mask_like(buildable_mask))

  coords_df <- data.frame(lon = high_risk_claims$lon_num, lat = high_risk_claims$lat_num)
  coords_df <- coords_df[!is.na(coords_df$lon) & !is.na(coords_df$lat), ]
  if (nrow(coords_df) == 0) return(false_mask_like(buildable_mask))

  claims_sf <- sf::st_as_sf(coords_df, coords = c("lon", "lat"), crs = 4326)
  claims_proj <- sf::st_transform(claims_sf, terra::crs(buildable_mask))
  debug_log(step, "Buffering", nrow(claims_proj), "claims by", buffer_distance, "m")
  claims_buffered <- sf::st_buffer(claims_proj, dist = buffer_distance)
  flood_raster <- terra::rasterize(terra::vect(claims_buffered), buildable_mask,
                                   field = 1, background = 0)
  flood_exclusion <- !is.na(flood_raster) & (flood_raster == 1)
  excluded_pixels <- sum(terra::values(flood_exclusion), na.rm = TRUE)
  total_pixels <- sum(!is.na(terra::values(flood_exclusion)))
  exclusion_percentage <- if (total_pixels > 0) (excluded_pixels / total_pixels) * 100 else 0
  debug_log(step, "Exclusion:", excluded_pixels, "/", total_pixels,
            "(", round(exclusion_percentage, 1), "%)")
  flood_exclusion
}

# =============================================================================
# MAIN: LARGEST CONTIGUOUS BUILDABLE LAND WITH TOGGLES AND IMPACT LOGGING
# =============================================================================
# Function: get_largest_contiguous_land
# Purpose:
#   Given a county GEOID, compute the buildable NLCD land area and apply optional
#   exclusions (wetlands, protected areas, slope, FEMA) to identify the largest
#   contiguous patch. Logs per-exclusion impacts and writes shapefile by default.
# Inputs:
#   county_geoid: character, county GEOID (e.g., "17031")
#   wetlands_include: logical; auto-fetch state wetlands via FIPS and subset to county
#   protected_include: logical; auto-fetch PAD-US for county
#   wetlands_data: optional sf override (pre-filtered wetlands)
#   protected_data: optional sf override (PAD-US)
#   include_slopes/include_floods: logical toggles for slope/FEMA
#   slope_threshold: numeric degrees (default 8; validated 0–45)
#   flood_buffer: numeric meters (default 300; validated 50–1000)
#   wetlands_buffer: numeric meters (default 100; validated 10–1000)
#   year: NLCD year (default 2019)
#   write_shapefile: logical; if TRUE, writes largest patch shapefile (default TRUE)
#   shapefile_dir: directory where shapefile folder per county is created (default "output")
# Outputs:
#   list with stats data.frame and largest_patch_polygon sf. Writes shapefile if enabled.

get_largest_contiguous_land <- function(
  county_geoid,
  wetlands_include = FALSE,
  protected_include = FALSE,
  wetlands_data = NULL,
  protected_data = NULL,
  include_slopes = FALSE,
  include_floods = FALSE,
  slope_threshold = 8,
  flood_buffer = 300,
  wetlands_buffer = 100,
  year = 2019,
  write_shapefile = TRUE,
  shapefile_dir = "shapefiles/land"  # Changed from "output"
) {
  main_step <- paste0("MAIN:", county_geoid)
  debug_log(main_step, "Processing county")

  # Validate buffers/threshold parameters to prevent extremes
  wetlands_buffer <- max(10, min(1000, wetlands_buffer))
  flood_buffer <- max(50, min(1000, flood_buffer))
  slope_threshold <- max(0, min(45, slope_threshold))
  debug_log(main_step, "Validated parameters: wetlands_buffer=", wetlands_buffer,
            "m, flood_buffer=", flood_buffer, "m, slope_threshold=", slope_threshold, "deg")

  # County AOI and state info
  counties_sf <- tigris::counties(cb = TRUE, resolution = "5m", year = 2023)
  county_poly <- counties_sf[counties_sf$GEOID == county_geoid, ]
  if (nrow(county_poly) == 0) {
    debug_log(main_step, "ERROR: County not found")
    return(NULL)
  }
  state_fips <- substr(county_geoid, 1, 2)
  state_code <- state_code_map[state_fips]
  debug_log(main_step, "County:", county_poly$NAME[1], ", State:", state_name_map[state_fips])

  # NLCD LandCover load (persistent cache) and buildable mask (categorical-safe)
  nlcd <- get_nlcd_landcover_spat(county_poly, year = year,
                                  label = paste0("County_", county_geoid),
                                  county_geoid = county_geoid)
  # Correction 3: Return NULL on NLCD failure (explicit failure signal)
  if (is.null(nlcd)) {
    debug_log(main_step, "NLCD unavailable; returning NULL (data failure)")
    return(NULL)
  }

  buildable_codes <- c(31, 71, 81, 82)  # barren, grassland, pasture, cultivated
  final_mask <- terra::ifel(nlcd %in% buildable_codes, 1L, 0L) == 1L
  final_mask_num <- terra::ifel(final_mask, 1L, 0L)

  px_res <- terra::res(nlcd)
  pixel_area_m2 <- abs(px_res[1] * px_res[2])
  pixel_area_acres <- pixel_area_m2 * 0.000247105

  baseline_pixels <- tryCatch(terra::global(final_mask_num, "sum", na.rm = TRUE)[1, 1],
                              error = function(e) sum(terra::values(final_mask_num), na.rm = TRUE))
  debug_log(main_step, "Baseline buildable pixels:", baseline_pixels,
            "| acres:", round(baseline_pixels * pixel_area_acres, 2))

  # Track pixels after each exclusion for reporting
  previous_pixels <- baseline_pixels

  # Wetlands exclusion
  county_wetlands <- NULL
  if (!is.null(wetlands_data)) {
    debug_log(main_step, "Using provided wetlands_data")
    county_wetlands <- wetlands_data
  } else if (wetlands_include) {
    if (!is.na(state_code)) {
      debug_log(main_step, "Auto-fetching state wetlands for", state_code)
      wet_ref <- get_state_wetlands(state_code)
      county_wetlands <- read_county_wetlands(wet_ref, county_poly)
      debug_log(main_step, "County wetlands features loaded:",
                if (!is.null(county_wetlands)) nrow(county_wetlands) else 0)
    }
  }

  if (!is.null(county_wetlands) && nrow(county_wetlands) > 0) {
    debug_log(main_step, "Applying wetlands exclusion | buffer =", wetlands_buffer, "m")
    wetlands_exclusion <- add_wetlands_exclusion(county_geoid, nlcd, county_wetlands,
                                                 buffer_distance = wetlands_buffer)
    final_mask <- final_mask & !wetlands_exclusion
    final_mask_num <- terra::ifel(final_mask, 1L, 0L)
    after_wetlands_pixels <- tryCatch(terra::global(final_mask_num, "sum", na.rm = TRUE)[1, 1],
                                      error = function(e) sum(terra::values(final_mask_num), na.rm = TRUE))
    delta <- previous_pixels - after_wetlands_pixels
    pct <- if (previous_pixels > 0) (delta / previous_pixels) * 100 else 0
    debug_log(main_step, "Impact wetlands: -", delta, "pixels (", round(pct, 2),
              "% ); acres delta:", round(delta * pixel_area_acres, 2))
    previous_pixels <- after_wetlands_pixels
  }

  # Protected areas exclusion
  padus_sf <- NULL
  if (!is.null(protected_data)) {
    debug_log(main_step, "Using provided protected_data")
    padus_sf <- protected_data
  } else if (protected_include) {
    debug_log(main_step, "Auto-fetching PAD-US")
    padus_sf <- get_county_protected_areas(county_geoid)
  }

  if (!is.null(padus_sf) && inherits(padus_sf, "sf")) {
    gap_field <- if ("GAP_Sts" %in% names(padus_sf)) "GAP_Sts" else
      if ("GAP_Status" %in% names(padus_sf)) "GAP_Status" else NA
    if (!is.na(gap_field)) {
      protected_to_exclude <- padus_sf[padus_sf[[gap_field]] %in% c("1", "2"), ]
      debug_log(main_step, "PAD-US exclude count (GAP 1/2):", nrow(protected_to_exclude))
      if (nrow(protected_to_exclude) > 0) {
        protected_transformed <- sf::st_transform(protected_to_exclude, terra::crs(nlcd))
        protected_raster <- terra::rasterize(terra::vect(protected_transformed), nlcd,
                                             field = 1, background = 0)
        protected_exclusion <- !is.na(protected_raster) & (protected_raster == 1)
        final_mask <- final_mask & !protected_exclusion
        final_mask_num <- terra::ifel(final_mask, 1L, 0L)
        after_padus_pixels <- tryCatch(terra::global(final_mask_num, "sum", na.rm = TRUE)[1, 1],
                                       error = function(e) sum(terra::values(final_mask_num), na.rm = TRUE))
        delta <- previous_pixels - after_padus_pixels
        pct <- if (previous_pixels > 0) (delta / previous_pixels) * 100 else 0
        debug_log(main_step, "Impact PAD-US: -", delta, "pixels (", round(pct, 2),
                  "% ); acres delta:", round(delta * pixel_area_acres, 2))
        previous_pixels <- after_padus_pixels
      }
    } else {
      debug_log(main_step, "GAP field missing; skipping PAD-US exclusion")
    }
  }

  # Slope exclusion (toggle)
  if (include_slopes) {
    debug_log(main_step, "Applying slope exclusion; threshold =", slope_threshold, "deg")
    slope_exclusion <- add_slope_exclusion(county_geoid, nlcd, slope_threshold)
    final_mask <- final_mask & !slope_exclusion
    final_mask_num <- terra::ifel(final_mask, 1L, 0L)
    after_slope_pixels <- tryCatch(terra::global(final_mask_num, "sum", na.rm = TRUE)[1, 1],
                                   error = function(e) sum(terra::values(final_mask_num), na.rm = TRUE))
    delta <- previous_pixels - after_slope_pixels
    pct <- if (previous_pixels > 0) (delta / previous_pixels) * 100 else 0
    debug_log(main_step, "Impact slope: -", delta, "pixels (", round(pct, 2),
              "% ); acres delta:", round(delta * pixel_area_acres, 2))
    previous_pixels <- after_slope_pixels
  } else {
    debug_log(main_step, "Skipping slope exclusion")
  }

  # FEMA flood exclusion (toggle)
  if (include_floods) {
    debug_log(main_step, "Applying FEMA flood exclusion; buffer =", flood_buffer, "m")
    flood_exclusion <- add_flood_exclusion_fema(county_geoid, nlcd, flood_buffer)
    final_mask <- final_mask & !flood_exclusion
    final_mask_num <- terra::ifel(final_mask, 1L, 0L)
    after_flood_pixels <- tryCatch(terra::global(final_mask_num, "sum", na.rm = TRUE)[1, 1],
                                   error = function(e) sum(terra::values(final_mask_num), na.rm = TRUE))
    delta <- previous_pixels - after_flood_pixels
    pct <- if (previous_pixels > 0) (delta / previous_pixels) * 100 else 0
    debug_log(main_step, "Impact FEMA: -", delta, "pixels (", round(pct, 2),
              "% ); acres delta:", round(delta * pixel_area_acres, 2))
    previous_pixels <- after_flood_pixels
  } else {
    debug_log(main_step, "Skipping FEMA flood exclusion")
  }

  # Final area metrics
  total_buildable_pixels <- tryCatch(terra::global(final_mask_num, "sum", na.rm = TRUE)[1, 1],
                                     error = function(e) sum(terra::values(final_mask_num), na.rm = TRUE))
  total_buildable_acres <- total_buildable_pixels * pixel_area_acres
  debug_log(main_step, "Final buildable pixels:", total_buildable_pixels)
  debug_log(main_step, "Pixel area (m^2):", round(pixel_area_m2, 2), "acres:", round(pixel_area_acres, 6))
  debug_log(main_step, "Final buildable acres:", round(total_buildable_acres, 2))

  # Connected patches (8-neighbor)
  debug_log(main_step, "Computing connected patches (8-neighbor)")
  patches_raster <- terra::patches(final_mask_num, directions = 8, zeroAsNA = TRUE)
  patch_table <- terra::freq(patches_raster)
  patch_table <- patch_table[!is.na(patch_table$value), , drop = FALSE]

  largest_patch_polygon <- NULL
  if (nrow(patch_table) == 0) {
    largest_acres <- 0
    num_components <- 0
    debug_log(main_step, "No patches found")
  } else {
    patch_table <- patch_table[order(patch_table$count, decreasing = TRUE), , drop = FALSE]
    largest_pixels <- patch_table$count[1]
    largest_acres <- largest_pixels * pixel_area_acres
    num_components <- nrow(patch_table)
    largest_patch_id <- patch_table$value[1]
    debug_log(main_step, "Largest patch id:", largest_patch_id, "pixels:", largest_pixels, "acres:", round(largest_acres, 2))

    largest_patch_mask <- patches_raster == largest_patch_id
    largest_patch_polygon_v <- terra::as.polygons(largest_patch_mask, values = TRUE, na.rm = TRUE)
    largest_patch_polygon <- sf::st_as_sf(largest_patch_polygon_v)
    att_col <- setdiff(names(largest_patch_polygon), "geometry")
    if (length(att_col) >= 1) {
      largest_patch_polygon <- largest_patch_polygon[largest_patch_polygon[[att_col[1]]] == 1, ]
    }
    if (nrow(largest_patch_polygon) > 1) {
      debug_log(main_step, "Unioning", nrow(largest_patch_polygon), "parts")
      u <- sf::st_union(largest_patch_polygon)
      u <- sf::st_make_valid(u)
      largest_patch_polygon <- sf::st_sf(patch = 1, geometry = u)
    }
  }

  debug_log(main_step, "Number of components:", if (exists("num_components")) num_components else 0)
  debug_log(main_step, "Largest contiguous area (acres):", round(if (exists("largest_acres")) largest_acres else 0, 2))

  # Optional shapefile writing (default TRUE) like OG code
  if (write_shapefile && !is.null(largest_patch_polygon)) {
    out_dir <- file.path(shapefile_dir, county_geoid)
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
    out_file <- file.path(out_dir, paste0("largest_patch_", county_geoid, ".shp"))
    sf::st_write(largest_patch_polygon, out_file, delete_layer = TRUE, quiet = TRUE)
    debug_log(main_step, "Saved shapefile:", out_file)
  }

  list(
    stats = data.frame(
      GEOID = county_geoid,
      county_name = county_poly$NAME[1],
      state = state_name_map[state_fips],
      total_buildable_acres = round(total_buildable_acres, 2),
      num_components = if (exists("num_components")) num_components else 0,
      largest_contiguous_acres = round(if (exists("largest_acres")) largest_acres else 0, 2),
      land_score = round(if (exists("largest_acres")) largest_acres else 0, 2)
    ),
    largest_patch_polygon = largest_patch_polygon
  )
}
