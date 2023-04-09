SPOTIFY_PODCAST_EPISODES = f"""
         SELECT COUNT(*) = 1 AS should_continue
                 FROM `singular-arcana-383119.refined_api.__TABLES__` 
                     WHERE row_count > 0 
                       AND CAST(TIMESTAMP_MILLIS(last_modified_time) AS DATE) = CURRENT_DATE('America/Sao_Paulo')
                       AND table_id  = 'spotify_podcast_episodes'
"""

SENSOR_QUERIES = {
    "spotify_podcast_episodes" : SPOTIFY_PODCAST_EPISODES
}