# =============================================================================
# STEP 1: GENERATE LAND SUITABILITY LAYER
# =============================================================================

setwd("C:/Users/mzullo/Desktop/datacenter")
source("land.R")
source("grid.R")
source("map.R")


cat("================================================================================\n")
cat("STEP 1: GENERATING LAND SUITABILITY LAYER FOR COOK COUNTY\n")
cat("================================================================================\n\n")

land_result <- get_largest_contiguous_land(
  county_geoid = "17031",
  wetlands_include = TRUE,
  protected_include = TRUE,
  include_slopes = TRUE,
  include_floods = TRUE,
  slope_threshold = 8,
  flood_buffer = 300,
  wetlands_buffer = 100,
  year = 2019,
  write_shapefile = TRUE,
  shapefile_dir = "shapefiles/land"
)

cat("\n[LAND] Results:\n")
print(land_result$stats)

# =============================================================================
# STEP 2: GENERATE GRID INFRASTRUCTURE LAYER
# =============================================================================

cat("\n\n")
cat("================================================================================\n")
cat("STEP 2: GENERATING GRID INFRASTRUCTURE LAYER FOR COOK COUNTY\n")
cat("================================================================================\n\n")

grid_result <- get_county_grid_layer(
  county_geoid = "17031",
  max_influence_distance = 25000,
  max_feasible_path_weight = 50,
  write_tiff = TRUE,
  output_dir = "shapefiles/grid"
)

cat("\n[GRID] Results:\n")
print(grid_result$stats)


# =============================================================================
# STEP 3: OVERLAY AND COMPARISON
# =============================================================================

cat("\n\n")
cat("================================================================================\n")
cat("STEP 3: OVERLAY ANALYSIS - LAND + GRID\n")
cat("================================================================================\n\n")

comparison_result <- compare_county_suitability(
  county_geoids = c("17031"),
  output_file = "cook_county_analysis.csv"
)

# =============================================================================
# STEP 4: VISUALIZE RESULTS
# =============================================================================

cat("\n\n")
cat("================================================================================\n")
cat("STEP 4: VISUALIZATION\n")
cat("================================================================================\n\n")


my_colors <- colorRampPalette(c("purple", "blue", "green", "yellow"))(100)


land_poly <- sf::st_read("shapefiles/land/17031/largest_patch_17031.shp", quiet = TRUE)
plot(grid_result$grid_raster[[1]], 
     main = "Cook County: N-1 Contingency + Buildable Land",
     col = my_colors)

plot(sf::st_geometry(land_poly), 
     border = "red", 
     lwd = 3, 
     add = TRUE)

# Add legend for buildable area
legend("bottomright", 
       legend = c("Buildable Land (397 acres)"),
       col = c("red"),
       lty = 1,
       lwd = 3,
       bg = "white",
       cex = 0.8)

# =============================================================================
# PLOT 2: GRID CONNECTIVITY (SHORTEST PATH) + BUILDABLE LAND
# =============================================================================

plot(grid_result$grid_raster[[2]], 
     main = "Cook County: Grid Connectivity (Shortest Path) + Buildable Land",
     col = my_colors)

plot(sf::st_geometry(land_poly), 
     border = "red", 
     lwd = 3, 
     add = TRUE)

# Add legend for buildable area
legend("bottomright", 
       legend = c("Buildable Land (397 acres)"),
       col = c("red"),
       lty = 1,
       lwd = 3,
       bg = "white",
       cex = 0.8)
