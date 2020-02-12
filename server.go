package main

import (
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/imatefx/transcoding/client"
	"github.com/imatefx/transcoding/ffmpeg"
	"github.com/labstack/echo"
	mw "github.com/labstack/echo/middleware"
	el "github.com/plutov/echo-logrus"
	"github.com/sevenNt/echo-pprof"
	log "github.com/sirupsen/logrus"
	"github.com/thoas/stats"
	"io"
	"io/ioutil"
	"net/http"
	//"net/http/pprof"
	"os"
	"strconv"
)

func TranscodeJsonPost(awsConfig AwsConfig, conversions map[string]FfmpegConversion) func(c echo.Context) error {
	fn := func(c echo.Context) error {
		request := &client.TranscodeRequest{}
		if err := c.Bind(request); err != nil {
			return err //return Unsupported Media Type or BadRequest
		}

		svc := s3.New(session.New(&aws.Config{Region: aws.String(awsConfig.Region)}))
		getObjectParams := &s3.GetObjectInput{
			Bucket: aws.String(request.Input.Bucket),
			Key:    aws.String(request.Input.Key),
		}
		resp, err := svc.GetObject(getObjectParams)
		if err != nil {
			log.WithFields(log.Fields{
				"error":   err.Error(),
				"code":    err.(awserr.Error).Code(),
				"message": err.(awserr.Error).Message(),
			}).Warn("Issue occured fetching object.")
			return err
		}
		input, err := ioutil.TempFile("", "s3Input")
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Error creating s3 input temporary file.")
			return err
		}
		defer os.Remove(input.Name())
		//Copy over the buffer to the file
		_, errCopy := io.Copy(input, resp.Body)
		if errCopy != nil {
			log.WithFields(log.Fields{
				"error": errCopy,
			}).Error("Error copying object to temporary input file")
			return errCopy
		}
		conversion, ok := conversions[request.Type]
		if !ok {
			return errors.New("This type does not exists")
		}
		output, err := ioutil.TempFile("", "output")
		if err != nil {
			return err
		}
		defer os.Remove(output.Name())
		converter := ffmpeg.NewConverter(input.Name(), output.Name(), conversion.Scale,
			conversion.VideoKilobitRate, conversion.AudioKilobitRate)

		if err := converter.Transcode(); err != nil {
			return err
		}

		fi, _ := output.Stat()
		//Begin Upload
		putObjectParams := &s3.PutObjectInput{
			Bucket:        aws.String(request.Output.Bucket),
			Key:           aws.String(request.Output.Key),
			ContentType:   aws.String("video/mp4"),
			ContentLength: aws.Int64(fi.Size()),
			Body:          output,
		}

		_, err1 := svc.PutObject(putObjectParams)
		if err1 != nil {
			log.WithFields(log.Fields{
				"error": err1,
			}).Error("Error putting s3 output temporary file.")
			return err1
		}
		return nil
	}
	return fn
}

/*
func TranscodeSQS(config AwsConfig, conversions map[string]FfmpegConversion) sqs.Handler {
	fn := func(msg *string) error {
		log.WithFields(log.Fields{
			"msg": *msg,
		}).Info("Received a SQS message.")
		var s3Event s3e.Event
		if err := json.Unmarshal([]byte(*msg), &s3Event); err != nil {
			log.WithFields(log.Fields{
				"msg": *msg,
			}).Warn("Received a SQS message we don't know how to handle. Consuming it.")
			return nil
		}

		svc := s3.New(session.New(&aws.Config{Region: aws.String(config.Region)}))
		for _, record := range s3Event.Records {

			if !strings.HasPrefix(record.EventName, "ObjectCreated") {
				log.WithFields(log.Fields{
					"type": record.EventName,
				}).Debug("Ignoring non-object created messages..")
				continue
			}

			getObjectParams := &s3.GetObjectInput{
				Bucket: aws.String(record.S3.Bucket.Name),
				Key:    aws.String(record.S3.Object.Key),
			}
			resp, err := svc.GetObject(getObjectParams)
			if err != nil {
				log.WithFields(log.Fields{
					"error":   err.Error(),
					"code":    err.(awserr.Error).Code(),
					"message": err.(awserr.Error).Message(),
				}).Warn("Issue occured fetching object.")
				return err
			}

			input, err := ioutil.TempFile("", "s3Input")
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Error("Error creating s3 input temporary file.")
				return err
			}
			defer os.Remove(input.Name())
			//Copy over the buffer to the file
			_, errCopy := io.Copy(input, resp.Body)
			if errCopy != nil {
				log.WithFields(log.Fields{
					"error": errCopy,
				}).Error("Error copying object to temporary input file")
				return errCopy
			}

			//TODO: perform multiple conversions based on metadata
			conversion, _ := conversions["320p"]
			output, err := ioutil.TempFile("", "output")
			if err != nil {
				return err
			}
			defer os.Remove(output.Name())
			converter := ffmpeg.NewConverter(input.Name(), output.Name(), conversion.Scale,
				conversion.VideoKilobitRate, conversion.AudioKilobitRate)

			if err := converter.Transcode(); err != nil {
				return err
			}

			fi, _ := output.Stat()
			//Begin Upload
			putObjectParams := &s3.PutObjectInput{
				Bucket:        aws.String(record.S3.Bucket.Name),
				Key:           aws.String("320p" + record.S3.Object.Key),
				ContentType:   aws.String("video/mp4"),
				ContentLength: aws.Int64(fi.Size()),
				Body:          output,
			}

			_, err1 := svc.PutObject(putObjectParams)
			if err1 != nil {
				log.WithFields(log.Fields{
					"error": err1,
				}).Error("Error putting s3 ouypuy temporary file.")
				return err1
			}

		}
		return nil
	}
	return sqs.HandlerFunc(fn)
}
*/

func TranscodeGet(c echo.Context) error {
	return c.File("./public/views/transcode.html")
}

func TranscodePost(conversions map[string]FfmpegConversion) echo.HandlerFunc {
	fn := func(c echo.Context) error {
		//The 0 here is important because it forces the file
		//to be written to disk, causing us to cast it to os.File
		c.Request().ParseMultipartForm(0)
		mf, _, err := c.Request().FormFile("input")
		if err != nil {
			c.String(http.StatusBadRequest, "Error parsing input file.")
			return err
		}
		input := mf.(*os.File)
		defer os.Remove(input.Name())

		output, err := ioutil.TempFile("", "output")
		if err != nil {
			c.String(http.StatusInternalServerError, "Error creating output file.")
			return err
		}
		defer os.Remove(output.Name())

		conversion, exists := conversions[c.FormValue("type")]

		log.WithFields(log.Fields{
			"conversionScale": conversion.Scale,
		}).Debug("-----------------------Test.")

		if !exists {
			return c.String(http.StatusBadRequest, "Not a valid transcoding type.")
		}

		converter := ffmpeg.NewConverter(input.Name(), output.Name(), conversion.Scale,
			conversion.VideoKilobitRate, conversion.AudioKilobitRate)

		if err := converter.Transcode(); err != nil {
			c.String(http.StatusInternalServerError, "Error transcoding the file.")
			return err
		}

		c.Response().Header().Set(echo.HeaderContentType, "video/mp4")
		fi, err := output.Stat()
		if err != nil {
			c.String(http.StatusInternalServerError, "Error retrieving size of file.")
			return err
		}
		c.Response().Header().Set(echo.HeaderContentLength, strconv.FormatInt(fi.Size(), 10))

		//if err := c.File(output.Name(), "output.mp4", true); err != nil {
		if err := c.File(output.Name()); err != nil {
			c.String(http.StatusInternalServerError, "Error sending file.")
			return err
		}

		return nil
	}
	return fn
}

func configHandler(config Config) echo.HandlerFunc {

	fn := func(c echo.Context) error {

		encoder := toml.NewEncoder(c.Response().Writer)
		if err := encoder.Encode(config); err != nil {
			return c.String(http.StatusInternalServerError, "Error parsing config file.")
			//http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		//w.WriteHeader(http.StatusOK)

		//c.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSONCharsetUTF8)
		//c.Response().WriteHeader(http.StatusOK)
		return toml.NewEncoder(c.Response()).Encode(config)
		//return json.NewEncoder(c.Response()).Encode(encoder)

		//return c.File(encoder.Encode(config))
	}

	return fn
}

func StartServer(config Config) {
	port := config.Server.Port
	debug := config.Server.Debug
	hostname := fmt.Sprintf(":%v", port)

	// Echo instance
	e := echo.New()

	//auto creating an index page for the directory
	//e.AutoIndex(true)

	//enable some helpful debug settings
	if debug {
		log.SetLevel(log.DebugLevel)
		e.Debug = debug
	}
	// https://github.com/thoas/stats
	s := stats.New()

	// Middleware

	el.Logger = log.New()
	e.Logger = el.GetEchoLogger()
	//e.Use(el.Hook())

	e.Use(
		el.Hook(),
		mw.Recover(),
		mw.Gzip(),
		//s.Handler,
	)

	/*
	*    Admin routes
	*   The following are some high level administration routes.
	 */
	admin := e.Group("/admin")
	admin.GET("", func(c echo.Context) error {
		return c.File("./public/views/admin.html")
	})
	//ping-pong
	admin.GET("/ping", func(c echo.Context) error {
		return c.String(http.StatusOK, "pong")
	})
	admin.GET("/stats", func(c echo.Context) error {
		return c.JSON(http.StatusOK, s.Data())
	})
	// Route to see the configuration we are using
	admin.GET("/config", configHandler(config))
	//pprof
	echopprof.Wrap(e)
	//admin.GET("/pprof", echopprof echo.HandlerFunc(pprof.Index))
	//admin.GET("/pprof/heap", pprof.Handler("heap").ServeHTTP)
	//admin.GET("/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	//admin.GET("/pprof/block", pprof.Handler("block").ServeHTTP)
	//admin.GET("/pprof/threadcreate", pprof.Handler("threadcreate").ServeHTTP)
	//admin.GET("/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	//admin.GET("/pprof/profile", http.HandlerFunc(pprof.Profile))
	//admin.GET("/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	//admin.GET("/pprof/trace", http.HandlerFunc(pprof.Trace))

	/*
	*   View routes
	*   The following are the view routes
	 */
	e.GET("/transcode", TranscodeGet)
	e.POST("/transcode", TranscodePost(config.Ffmpeg.Conversions))

	/*
	*   API routes
	*   The following are the API routes
	 */
	g := e.Group("/api")
	g.POST("/transcode", TranscodeJsonPost(config.Aws, config.Ffmpeg.Conversions))

	// Start server
	log.WithFields(log.Fields{
		"port":  port,
		"debug": debug,
	}).Info("Starting the server...")
	for _, route := range e.Routes() {
		log.Info(route.Method + " " + route.Path)
	}
	e.Start(hostname)
}
