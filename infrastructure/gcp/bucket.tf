provider "google" {
  project = var.igti-edc-mod4
  region = var.region
}

resource = "google_storage_bucket" "my_bucket"{
  name = var.mateus-landing-zone
  location = var.region
}


resource = "google_storage_bucket" "my_bucket"{
  name = var.mateus-mateus-processing-zone
  location = var.region
}


resource = "google_storage_bucket" "my_bucket"{
  name = var.mateus-serving-zone
  location = var.region
}


