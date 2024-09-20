library(dplyr)
library(readr)

Collaborator <- list(

  init = function() {
    list(
      SS_data = read_delim('input_data/gmag_br_barySS.txt', delim = ' '),
      NST_data = read_delim('input_data/gmag_br_baryNST.txt', delim = ' ')
    )
  },
  
  collaborate = function(self, astrometry_df, catalogue = 'GDR2toGDR3', mode='SS', radec_unit = 'deg') {
    astrometry_df_this <- astrometry_df
    if (catalogue == 'GDR2toGDR3') {
      if (mode == 'SS') {
        this_collaboration_data <- self$SS_data
      } else {
        this_collaboration_data <- self$NST_data
      }

      coeff <- this_collaboration_data %>% 
        filter(mag1 <= astrometry_df_this$mag, mag2 > astrometry_df_this$mag, br1 <= astrometry_df_this$br, br2 > astrometry_df_this$br) %>% 
        select(c('ex23', 'ey23', 'ez23', 'ox23', 'oy23', 'oz23', 'plx23', 'a3', 'b3', 'a2', 'b2', 'a1', 'b1'))

      if (nrow(coeff) != 1) {
        print('mag,br out of range!')
        return(list(ra = -99999999, dec = -99999999, plx = -99999999, pmra = -99999999, pmdec = -99999999))
      }

      coeff <- as.list(coeff)
      coeff['ex23'] <- -as.numeric(coeff['ex23']) / 206264.80624709636 / 1000
      coeff['ey23'] <- -as.numeric(coeff['ey23']) / 206264.80624709636 / 1000
      coeff['ez23'] <- -as.numeric(coeff['ez23']) / 206264.80624709636 / 1000
      coeff['ox23'] <- -as.numeric(coeff['ox23']) / 206264.80624709636 / 1000
      coeff['oy23'] <- -as.numeric(coeff['oy23']) / 206264.80624709636 / 1000
      coeff['oz23'] <- -as.numeric(coeff['oz23']) / 206264.80624709636 / 1000
      coeff['plx23'] <- -as.numeric(coeff['plx23'])
      coeff['a3'] <- as.numeric(coeff['a3'])
      coeff['a2'] <- as.numeric(coeff['a2'])
      coeff['a1'] <- as.numeric(coeff['a1'])
      coeff['b3'] <- as.numeric(coeff['b3'])
      coeff['b2'] <- as.numeric(coeff['b2'])
      coeff['b1'] <- as.numeric(coeff['b1'])

      beta <- matrix(c(
  as.numeric(coeff['ex23']), as.numeric(coeff['ey23']), as.numeric(coeff['ez23']),
  as.numeric(coeff['ox23']), as.numeric(coeff['oy23']), as.numeric(coeff['oz23']),
  as.numeric(coeff['plx23'])
), ncol = 1)
      Dt <- -0.5
    }
    if (catalogue == "HIPtoVLBI2015") {
    beta <- matrix(c(0, 0, 0, 0.126/206264.80624709636/1000,
                    -0.185/206264.80624709636/1000,
                    -0.076/206264.80624709636/1000, 0.089), ncol = 1)
    Dt <- 23.75
    }
    if (catalogue == "VLBI2015toVLBI2020") {
    beta <- matrix(c(0.008/206264.80624709636/1000,
                    0.015/206264.80624709636/1000,
                    0/206264.80624709636/1000, 0, 0, 0, 0), ncol = 1)
    Dt <- 5.015
    }
    if (catalogue == "VLBI2020toGDR3") {
    beta <- matrix(c(0.226/206264.80624709636/1000,
                    0.327/206264.80624709636/1000,
                    0.168/206264.80624709636/1000,
                    0.022/206264.80624709636/1000,
                    0.065/206264.80624709636/1000,
                    -0.016/206264.80624709636/1000, 0), ncol = 1)
    Dt <- 4.015
    }
    if (catalogue == "HIPtoGDR3") {
    resVLBI15 <- Collaborator.collaborate(self,astrometry_df, catalogue = "HIPtoVLBI2015")
    resVLBI20 <- Collaborator.collaborate(self,resVLBI15, catalogue = "VLBI2015toVLBI2020")
    res <- Collaborator.collaborate(self,resVLBI20, catalogue = "VLBI2020toGDR3")
    return(res)
    }

    if (catalogue == "GDR1toGDR3") {
    if (mode == "NSTP2") {
        beta <- matrix(c(0/206264.80624709636/1000,
                        -0.13/206264.80624709636/1000,
                        -0.01/206264.80624709636/1000, 0,
                        0/206264.80624709636/1000, 0,
                        0), ncol = 1)
    }
    if (mode == "SSP5") {
        beta <- matrix(c(0.39/206264.80624709636/1000,
                        -0.17/206264.80624709636/1000,
                        0.12/206264.80624709636/1000,
                        0.02/206264.80624709636/1000,
                        -0.03/206264.80624709636/1000,
                        0.02/206264.80624709636/1000, 0), ncol = 1)
    }
    Dt <- 1
    }

    # Additional code blocks for other catalogue conditions would go here

    if (radec_unit== 'deg') {
      ra <- astrometry_df_this$ra*pi/180
      dec <- astrometry_df_this$dec*pi/180
        tryCatch({
        pmra <- astrometry_df_this$pmra/206264.80624709636/1000  # mas/yr -> rad/yr
        pmdec <- astrometry_df_this$pmdec/206264.80624709636/1000  # mas/yr -> rad/yr
        have_pm <- 1
        if (abs(astrometry_df_this$pmra+99999999<100) || abs(astrometry_df_this$pmdec+99999999)<100) {
            print('no pm data!')
            have_pm <- 0
        }
        }, error = function(e) {
        pmra <- 0
        pmdec <- 0
        have_pm <- 0
        })

        tryCatch({
        plx <- astrometry_df_this$plx
        have_Plx <- 1
        if (abs(plx+99999999)<100) {
            have_Plx <- 0
        }
        }, error = function(e) {
        plx <- 0
        have_Plx <- 0
        })
      # Additional code blocks for conditions would go here
    }

    Kappa <- matrix(c(
      cos(ra)*sin(dec),sin(ra)*sin(dec),-cos(dec),Dt*cos(ra)*sin(dec),Dt*sin(ra)*sin(dec),-Dt*cos(dec),0,
      -sin(ra),cos(ra),0,-Dt*sin(ra),Dt*cos(ra),0,0,
      0,0,0,0,0,0,1,
      0,0,0,cos(ra)*sin(dec),sin(ra)*sin(dec),-cos(dec),0,
      0,0,0,-sin(ra),cos(ra),0,0), nrow = 5, byrow = TRUE)

    astro_origin <- matrix(c(ra*cos(dec),dec,plx,pmra,pmdec), ncol = 1)

    beta <- as.matrix(data.frame(beta),ncol = 1)
    collaborated_astrometry <- astro_origin - Kappa %*% beta
    res <- list(ra = collaborated_astrometry[1]/cos(dec)*180/pi, 
                dec = collaborated_astrometry[2]*180/pi, 
                plx = collaborated_astrometry[3]*have_Plx-(1-have_Plx)*99999999, 
                pmra = have_pm*collaborated_astrometry[4]*206264.80624709636*1000-(1-have_pm)*99999999, 
                pmdec = have_pm*collaborated_astrometry[5]*206264.80624709636*1000-(1-have_pm)*99999999)

    return(res)
  }
)

# Instantiate the object in R using the following code
collab <- Collaborator$init()

astrometry_df <- list(
  ra = 45.34,
  dec = -23.56,
  pmra =  -99999999,
  pmdec = -0.001,
  plx = -99999999,
  mag = 10.3,
  br = 0.5
)


result <- Collaborator$collaborate(collab, astrometry_df, catalogue = 'GDR2toGDR3', mode = 'SS', radec_unit = 'deg')

print(result)