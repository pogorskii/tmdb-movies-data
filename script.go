package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/time/rate"
)

type Movie struct {
	ID 									int						`json:"id"`
	OriginalLanguage    string        `json:"original_language"`
  OriginalTitle     	string        `json:"original_title"`
	Title     					string        `json:"title"`
	PosterPath          *string       `json:"poster_path"`
	Popularity         	float64				`json:"popularity"`
	Runtime             int           `json:"runtime"`
  Budget              int           `json:"budget"`
	ReleaseDate         string        `json:"release_date"`
	Releases        		[]Release  		`json:"release_dates"`
	Genres              []int         `json:"genres"`
	ProductionCountries []Country			`json:"production_countries"`
	Actors 							[]Actor				  `json:"actors"`
	Directors						[]Director		      `json:"directors"`
}

type Country struct {
  ISO3166_1 string `json:"iso_3166_1"`
  Name      string `json:"name"`
}

type Release struct {
  ISO639_1    string   `json:"iso_639_1"`
	LocalReleaseDates []LocalReleaseDate `json:"local_release_dates"`
}

type LocalReleaseDate struct {
	Note        string   `json:"note"`
  ReleaseDate string   `json:"release_date"`
  Type        int      `json:"type"`
}

type Actor struct {
	ID						int				`json:"id"`
	Name 					string 		`json:"name"`
	Order					int				`json:"order"`
}

type Director struct {
	ID						int				`json:"id"`
	Name 					string 		`json:"name"`
}

type MovieID struct {
  ID int `json:"id"`
}

var (
  // Rate limiter with 50 requests per second and 1 burst token
  limiter = rate.NewLimiter(rate.Every(time.Second/50), 1)

  // Mutex to protect access to processed movie data
  processedMovieDataMutex sync.Mutex
  processedMovieData map[int]*Movie

  // Channels to handle movie IDs and processed data
  movieIDChannel chan int
  processedMovieDataChannel chan Movie

  // Number of worker goroutines
  numWorkers int
)

func fetchMovieData(movieID int) ([]byte, error) {
  // Build the API URL
  url := fmt.Sprintf("https://api.themoviedb.org/3/movie/%d?append_to_response=release_dates%%2Ccredits&language=en-US", movieID)

  // Create the HTTP request
  req, err := http.NewRequest("GET", url, nil)
  if err != nil {
    return nil, err
  }

  // Set headers and authorization
  apiKey := os.Getenv("API_ACCESS_TOKEN")
  authorizationValue := fmt.Sprintf("Bearer %s", apiKey)
  req.Header.Add("accept", "application/json")
  req.Header.Add("Authorization", authorizationValue)

  // Make the HTTP request and handle the response
  client := http.DefaultClient
  res, err := client.Do(req)
  if err != nil {
    return nil, err
  }
  defer res.Body.Close()

  // Check for successful response
  if res.StatusCode != http.StatusOK {
    return nil, fmt.Errorf("unexpected HTTP status code: %d", res.StatusCode)
  }

  // Read the response body
  body, err := io.ReadAll(res.Body)
  if err != nil {
    return nil, err
  }

  return body, nil
}

func processMovieData(rawData map[string]interface{}) (*Movie, error) {
  movie := &Movie{}

  // Extract and map basic movie information
  for key, value := range rawData {
    switch key {
    case "id":
      movie.ID = int(rawData["id"].(float64))
    case "original_language":
      movie.OriginalLanguage = value.(string)
    case "original_title":
      movie.OriginalTitle = value.(string)
		case "title":
      movie.Title = value.(string)
		case "poster_path":
			if value == nil {
				movie.PosterPath = nil // Set to an empty string if nil
			} else {
				movie.PosterPath = new(string) // Allocate memory for string
      	*movie.PosterPath = value.(string) // Store the actual path
			}
		case "popularity":
			movie.Popularity = value.(float64)
		case "runtime":
      movie.Runtime = int(rawData["runtime"].(float64))
    case "budget":
      movie.Budget = int(rawData["budget"].(float64))
		case "release_date":
			movie.ReleaseDate = rawData["release_date"].(string)
		}
  }

  // Extract and parse nested "release_dates" object
  releaseDates, ok := rawData["release_dates"]
  if ok {
    releaseDatesMap, ok := releaseDates.(map[string]interface{})
    if ok {
      movie.Releases = parseReleaseDates(releaseDatesMap)
    }
  }

  genres, ok := rawData["genres"]
  if ok {
    genresMap, ok := genres.([]interface{})
    if ok {
      movie.Genres = parseGenres(genresMap)
    }
  }

  productionCountries, ok := rawData["production_countries"]
  if ok {
    countriesMap, ok := productionCountries.([]interface{})
    if ok {
      movie.ProductionCountries = parseProductionCountries(countriesMap)
    }
  }

	// Extract and parse nested "credits" object
	credits, ok := rawData["credits"]
	if ok {
		credits, ok := credits.(map[string]interface{})
		if ok {
			movie.Actors = parseActors(credits)
			movie.Directors = parseDirectors(credits)
		}
	}

  return movie, nil
}

func parseReleaseDates(releaseDatesMap map[string]interface{}) []Release {
  var releaseDates []Release

  // Pre-allocate memory for the slice
  releaseDates = make([]Release, 0, len(releaseDatesMap["results"].([]interface{})))

  // Loop through each language release data
  for _, languageData := range releaseDatesMap["results"].([]interface{}) {
    languageMap, ok := languageData.(map[string]interface{})
    if !ok {
      continue
    }

    // Extract ISO code and local release dates
    isoCode := languageMap["iso_3166_1"].(string)
    localReleaseDatesMap, ok := languageMap["release_dates"].([]interface{})
    if !ok {
      continue
    }

    // Parse local release date information
    var localReleaseDates []LocalReleaseDate
    for _, releaseDateMap := range localReleaseDatesMap {
      localReleaseDate, err := parseLocalReleaseDate(releaseDateMap.(map[string]interface{}))
      if err != nil {
        continue
      }
      localReleaseDates = append(localReleaseDates, *localReleaseDate)
    }

    releaseDates = append(releaseDates, Release{
      ISO639_1: isoCode,
      LocalReleaseDates: localReleaseDates,
    })
  }

  return releaseDates
}

func parseGenres(genresMap []interface{}) []int {
  var genres []int

  genres = make([]int, 0, len(genresMap))

  for _, genreData := range genresMap {
    genreMap, ok := genreData.(map[string]interface{})
    if !ok {
      continue
    }

    genres = append(genres, int(genreMap["id"].(float64)))
  }

  return genres
}

func parseProductionCountries(countriesMap []interface{}) []Country {
  var countries []Country

  countries = make([]Country, 0, len(countriesMap))

  for _, countryData := range countriesMap {
    countryMap, ok := countryData.(map[string]interface{})
    if !ok {
      continue
    }

    ISO3166_1 := countryMap["iso_3166_1"].(string)
    name := countryMap["name"].(string)

    countries = append(countries, Country{
      ISO3166_1: ISO3166_1,
      Name: name,
    })
  }

  return countries
}

func parseActors(actorsMap map[string]interface{}) []Actor {
	var actors []Actor

	for _, actorData := range actorsMap["cast"].([]interface{}) {
		actorMap, ok := actorData.(map[string]interface{})
		if !ok {
			continue
		}

    order := int(actorMap["order"].(float64))

    if order < 5 {
      // Update the actor slice capacity to accommodate a new element
      actors = append(actors, Actor{}) // Append a zero-initialized element
      lastActorIndex := len(actors) - 1

      // Populate the newly added element
      actors[lastActorIndex].ID = int(actorMap["id"].(float64))
      actors[lastActorIndex].Name = actorMap["name"].(string)
      actors[lastActorIndex].Order = order
    }
	}

	return actors
}


func parseDirectors(directorsMap map[string]interface{}) []Director {
	var directors []Director

  // Pre-allocate memory for the slice
  directors = make([]Director, 0, len(directorsMap["crew"].([]interface{})))

	for _, directorData := range directorsMap["crew"].([]interface{}) {
		directorMap, ok := directorData.(map[string]interface{})
		if !ok {
			continue
		}

    if directorMap["job"] == "Director" {
      id := int(directorMap["id"].(float64))
		  name := directorMap["name"].(string)

      directors = append(directors, Director{
        ID: id,
        Name: name,
      })
    }
	}

	return directors
}


func parseLocalReleaseDate(releaseDateMap map[string]interface{}) (*LocalReleaseDate, error) {
  localReleaseDate := &LocalReleaseDate{}

  // Assign extracted values
  localReleaseDate.Note = releaseDateMap["note"].(string)
  localReleaseDate.ReleaseDate = releaseDateMap["release_date"].(string)
  localReleaseDate.Type = int(releaseDateMap["type"].(float64))

  return localReleaseDate, nil
}

func fetchAndProcessMovieData(movieIDChannel chan int, processedMovieDataChannel chan Movie) {
  for movieID := range movieIDChannel {
    // Apply rate limiting
    if err := limiter.Wait(context.Background()); err != nil {
      fmt.Printf("Rate limit exceeded for movie ID %d: %v\n", movieID, err)
      continue
    }

    // Fetch and process movie data
    body, err := fetchMovieData(movieID)
    if err != nil {
      fmt.Printf("Error fetching movie data for ID %d: %v\n", movieID, err)
      continue
    }

    var rawData map[string]interface{}
    err = json.Unmarshal(body, &rawData)
    if err != nil {
      fmt.Println(err)
      continue
    }

    processedData, err := processMovieData(rawData)
    if err != nil {
      fmt.Println(err)
      continue
    }

    // Send processed data back to the main goroutine
    processedMovieDataChannel <- *processedData
  }
}

var writeTimeout = 410 * time.Second // Define a timeout for writing

func main() {
  // Load the .env file automatically
  err := godotenv.Load()
  if err != nil {
    fmt.Println("Error loading .env file:", err)
    return
  }

  timestamp := time.Now().Format("15:04:05")
  fmt.Printf("Started executing at %s \n", timestamp)

  // Set the number of worker goroutines
  numWorkers = 500

  // Initialize channels with appropriate buffer size
  movieIDChannel = make(chan int, 10000000)
  processedMovieDataChannel = make(chan Movie, 1000000)

  // Create a separate channel to signal worker completion
  workerDone := make(chan struct{})
  // Create a single channel to signal both worker completion and write completion
  done := make(chan struct{})

  // Spawn worker goroutines for fetching and processing data
  var wg sync.WaitGroup
  wg.Add(numWorkers)

  // Spawn worker goroutines for fetching and processing data
  for i := 0; i < numWorkers; i++ {
    go func() {
      fetchAndProcessMovieData(movieIDChannel, processedMovieDataChannel)
      wg.Done() // Signal worker finished processing
    }()
  }

  // Read movie IDs from JSON file
  movieIDs, err := readMovieIDsFromFile("movie_ids.json")
  if err != nil {
    fmt.Println(err)
    return
  }

  // Send movie IDs to the channel for processing
  for _, movieID := range movieIDs {
    movieIDChannel <- movieID.ID
  }

  // Close movie ID channel after sending all IDs
  close(movieIDChannel)

  // Launch a separate goroutine to monitor worker completion
  go func() {
    wg.Wait()
    close(workerDone)
  }()

  var processedMovieData []Movie
  var processedMovieDataMutex sync.Mutex

  const batchSize = 100
  var batchBuffer []Movie

  var wgWrite sync.WaitGroup

  // Write data in batches
  go func() {
    defer wgWrite.Done() // Signal no more batches will be written

    timer := time.NewTimer(writeTimeout)
    defer timer.Stop() // Stop timer when done

    for {
      select {
      case processedData := <-processedMovieDataChannel:
        processedMovieDataMutex.Lock()
        processedMovieData = append(processedMovieData, processedData)
        batchBuffer = append(batchBuffer, processedData)
        processedMovieDataMutex.Unlock()

        if len(batchBuffer) >= batchSize {
          wgWrite.Add(1)
          timestamp := time.Now().Format("15:04:05")
          fmt.Printf("Writing data for batch with the size of %d at %s \n", len(batchBuffer), timestamp)
          err := writeMovieDataToJSONFile(batchBuffer)
          if err != nil {
            fmt.Println(err)
            return
          }

          timer.Reset(writeTimeout) // Reset timer on successful write

          wgWrite.Done()

          // Clear batch buffer
          batchBuffer = []Movie{}
        }
      case <-timer.C:
        wgWrite.Add(1)
        // Write any remaining data after timeout
        if len(batchBuffer) > 0 {
          timestamp := time.Now().Format("15:04:05")
          fmt.Printf("Writing data for batch with the size of %d at %s \n", len(batchBuffer), timestamp)
          err := writeMovieDataToJSONFile(batchBuffer)
          if err != nil {
            fmt.Println(err)
            return
          }

          // Clear batch buffer
          batchBuffer = []Movie{}
        }
        // Signal program completion after writing remaining data
        wgWrite.Done()
        close(done)
      }
    }
    
  }()
  
  // Wait for the program completion signal from the write goroutine
  <-done
  wgWrite.Wait() // Wait for all write operations to finish
  close(processedMovieDataChannel) // Close channel after all data is processed

  fmt.Println("Successfully processed and saved movie data")
}

func readMovieIDsFromFile(filename string) ([]MovieID, error) {
  // Open the file
  file, err := os.Open(filename)
  if err != nil {
    return nil, err
  }
  defer file.Close()

  // Read the file content
  data, err := io.ReadAll(file)
  if err != nil {
    return nil, err
  }

  // Declare a slice of MovieID objects
  var movieIDs []MovieID

  // Unmarshal the data into the slice
  err = json.Unmarshal(data, &movieIDs)
  if err != nil {
    return nil, err
  }

  return movieIDs, nil
}

func writeMovieDataToJSONFile(data []Movie) error {
  // Generate a unique filename based on current timestamp
  timestamp := time.Now().Format("15-04-05")
  filename := fmt.Sprintf("processed_movies_%s.json", timestamp)

  // Open the file for writing
  file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
  if err != nil {
    return err
  }
  defer file.Close()

  // Encode the data to JSON format
  encoder := json.NewEncoder(file)

  // Write the data to the file
  err = encoder.Encode(data)
  if err != nil {
    return err
  }

  return nil
}