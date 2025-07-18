package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/pkg/sftp"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"golang.org/x/crypto/ssh"
)

type Config struct {
	SFTPUser           string
	SFTPPassword       string
	SFTPHost           string
	SFTPPort           string
	SFTPDir            string
	HeadersFileName    string
	ArchiveDir         string
	KafkaBrokers       string
	KafkaTopic         string
	KafkaUsername      string
	KafkaPassword      string
	KafkaSaslMechanism string
	KafkaCACertPath    string
	PollInterval       time.Duration
}

func main() {
	log.Printf("=== INICIANDO PIPELINE GO SFTP -> KAFKA ===")

	// Carrega as variáveis de ambiente do .env
	if err := godotenv.Load(); err != nil {
		log.Fatal("Erro ao carregar .env")
	}

	cfg := loadEnvVars()
	printEnvDebug(cfg)

	for {
		log.Printf("[ETAPA] Iniciando ciclo de processamento...")
		err := processAllTxtFiles(cfg)
		if err != nil {
			log.Printf("[ERRO] Processamento falhou: %v", err)
		}
		log.Printf("[ETAPA] Fim do ciclo. Aguardando %ds...", int(cfg.PollInterval.Seconds()))
		time.Sleep(cfg.PollInterval)
	}
}

func loadEnvVars() Config {
	return Config{
		SFTPUser:           os.Getenv("SFTP_USER"),
		SFTPPassword:       os.Getenv("SFTP_PASSWORD"),
		SFTPHost:           os.Getenv("SFTP_HOST"),
		SFTPPort:           os.Getenv("SFTP_PORT"),
		SFTPDir:            os.Getenv("SFTP_DIR"),
		HeadersFileName:    os.Getenv("SFTP_HEADERS_FILENAME"),
		ArchiveDir:         os.Getenv("SFTP_ARCHIVE_DIR"),
		KafkaBrokers:       os.Getenv("KAFKA_BROKERS"),
		KafkaTopic:         os.Getenv("KAFKA_TOPIC"),
		KafkaUsername:      os.Getenv("KAFKA_USERNAME"),
		KafkaPassword:      os.Getenv("KAFKA_PASSWORD"),
		KafkaSaslMechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
		KafkaCACertPath:    os.Getenv("KAFKA_CA_CERT_PATH"),
		PollInterval:       10 * time.Second,
	}
}

func printEnvDebug(cfg Config) {
	log.Printf("==== DEBUG VARS ====")
	log.Printf("SFTP_USER: %s", cfg.SFTPUser)
	log.Printf("KAFKA_BROKERS: %s", cfg.KafkaBrokers)
	log.Printf("KAFKA_TOPIC: %s", cfg.KafkaTopic)
	log.Printf("KAFKA_USERNAME: %s", cfg.KafkaUsername)
	log.Printf("KAFKA_SASL_MECHANISM: %s", cfg.KafkaSaslMechanism)
	log.Printf("KAFKA_CA_CERT_PATH: %s", cfg.KafkaCACertPath)
	log.Printf("====================")
}

func createKafkaWriter(cfg Config) *kafka.Writer {
	mechanism, err := scram.Mechanism(scram.SHA256, cfg.KafkaUsername, cfg.KafkaPassword)
	if err != nil {
		log.Fatalf("Erro mecanismo SCRAM: %v", err)
	}

	caCert, err := os.ReadFile(cfg.KafkaCACertPath)
	if err != nil {
		log.Fatalf("Erro lendo CA cert: %v", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		log.Fatal("Erro adicionando CA cert ao pool")
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           &tls.Config{RootCAs: caPool},
		SASLMechanism: mechanism,
	}

	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      strings.Split(cfg.KafkaBrokers, ","),
		Topic:        cfg.KafkaTopic,
		Async:        true,
		BatchSize:    1000,
		BatchTimeout: 500 * time.Millisecond,
		Dialer:       dialer,
	})
}

func processAllTxtFiles(cfg Config) error {
	log.Printf("[ETAPA] Conectando ao SFTP...")
	sftpClient, err := connectSFTP(cfg)
	if err != nil {
		return fmt.Errorf("[ERRO] Erro conectando SFTP: %w", err)
	}
	defer sftpClient.Close()
	log.Printf("[OK] SFTP conectado!")

	log.Printf("[ETAPA] Lendo diretório remoto: %s", cfg.SFTPDir)
	files, err := sftpClient.ReadDir(cfg.SFTPDir)
	if err != nil {
		return fmt.Errorf("[ERRO] Erro lendo diretório SFTP: %w", err)
	}
	log.Printf("[OK] Arquivos encontrados: %v", fileNames(files))

	countFiles := 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(strings.ToLower(file.Name()), ".txt") {
			countFiles++
			log.Printf("[ETAPA] Processando arquivo: %s", file.Name())
			err := processFile(cfg, sftpClient, file.Name())
			if err != nil {
				log.Printf("[ERRO] Erro processando arquivo [%s]: %v", file.Name(), err)
			}
		}
	}

	if countFiles == 0 {
		log.Printf("[ATENÇÃO] Nenhum arquivo .txt encontrado em %s", cfg.SFTPDir)
	}
	return nil
}

func processFile(cfg Config, sftpClient *sftp.Client, fileName string) error {
	filePath := cfg.SFTPDir + fileName
	headersPath := cfg.SFTPDir + cfg.HeadersFileName

	log.Printf("[ETAPA] Abrindo arquivo de dados: %s", filePath)
	file, err := sftpClient.Open(filePath)
	if err != nil {
		return fmt.Errorf("[ERRO] Abrindo arquivo [%s]: %w", fileName, err)
	}
	defer file.Close()

	var header []string
	log.Printf("[ETAPA] Buscando headers em: %s", headersPath)
	header, err = loadHeaders(sftpClient, headersPath)
	if err != nil {
		log.Printf("[ATENÇÃO] Não encontrou headers.txt, usando headers automáticos para [%s]...", fileName)
		reader := csv.NewReader(file)
		reader.Comma = ';'
		firstLine, err2 := reader.Read()
		if err2 != nil {
			return fmt.Errorf("[ERRO] Lendo primeira linha para gerar headers: %w", err2)
		}
		header = make([]string, len(firstLine))
		for i := range firstLine {
			header[i] = fmt.Sprintf("field%d", i+1)
		}
		_, errSeek := file.Seek(0, io.SeekStart)
		if errSeek != nil {
			return fmt.Errorf("[ERRO] Ao voltar ponteiro do arquivo: %w", errSeek)
		}
	}
	log.Printf("[OK] Headers em uso para [%s]: %+v", fileName, header)
	log.Printf("[ETAPA] Iniciando leitura e envio para Kafka: %s", cfg.KafkaTopic)

	writer := createKafkaWriter(cfg)
	defer writer.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	linha := 1
	start := time.Now()
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[ERRO] Erro lendo linha %d: %v", linha, err)
			continue
		}
		rowMap := mapLine(header, record)
		ordered := make([]string, 0, len(header))
		for _, k := range header {
			v := rowMap[k]
			v = strings.ReplaceAll(v, "\"", "\\\"")
			ordered = append(ordered, fmt.Sprintf("\"%s\":\"%s\"", k, v))
		}
		jsonStr := "{" + strings.Join(ordered, ",") + "}"
		jsonBytes := []byte(jsonStr)

		err = writer.WriteMessages(context.Background(),
			kafka.Message{Value: jsonBytes})
		if err != nil {
			log.Printf("[ERRO] Erro enviando pro Kafka linha %d: %v", linha, err)
		}
		if linha%500000 == 0 {
			log.Printf("[PARCIAL] Já processou %d linhas do arquivo [%s]!", linha, fileName)
		}
		linha++
	}

	log.Printf("[OK] Total de linhas lidas do arquivo [%s]: %d", fileName, linha-1)
	log.Printf("[OK] Tempo total para processar arquivo [%s]: %s", fileName, time.Since(start))

	// Move para archive com nome incremental!
	log.Printf("[ETAPA] Movendo arquivo [%s] para archive...", fileName)
	err = sftpClient.MkdirAll(cfg.ArchiveDir)
	if err != nil {
		log.Printf("[ERRO] Criando diretório archive: %v", err)
	}
	archivePath, err := findAvailableArchiveName(sftpClient, cfg.ArchiveDir, fileName)
	if err != nil {
		log.Printf("[ERRO] Encontrando nome disponível para archive: %v", err)
		return err
	}

	err = sftpClient.Rename(filePath, archivePath)
	if err != nil {
		log.Printf("[ERRO] Movendo arquivo pra archive: %v", err)
		log.Printf("[ETAPA] Tentando copiar + deletar como fallback...")

		src, err1 := sftpClient.Open(filePath)
		if err1 != nil {
			log.Printf("[ERRO] Abrindo arquivo para cópia: %v", err1)
			return err
		}
		defer src.Close()
		dst, err2 := sftpClient.Create(archivePath)
		if err2 != nil {
			log.Printf("[ERRO] Criando arquivo de destino na archive: %v", err2)
			return err
		}
		defer dst.Close()
		_, err3 := io.Copy(dst, src)
		if err3 != nil {
			log.Printf("[ERRO] Copiando arquivo: %v", err3)
			return err
		}
		err4 := sftpClient.Remove(filePath)
		if err4 != nil {
			log.Printf("[ERRO] Deletando arquivo original após cópia: %v", err4)
			return err
		}
		log.Printf("[OK] Arquivo copiado para archive e removido do diretório original! (%s)", archivePath)
	} else {
		log.Printf("[OK] Arquivo [%s] movido para %s", fileName, archivePath)
	}
	log.Printf("[ETAPA] Fim do processamento do arquivo [%s]", fileName)
	return nil
}

func fileNames(files []os.FileInfo) []string {
	var names []string
	for _, f := range files {
		names = append(names, f.Name())
	}
	return names
}

func findAvailableArchiveName(sftpClient *sftp.Client, archiveDir, fileName string) (string, error) {
	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	for i := 0; ; i++ {
		var name string
		if i == 0 {
			name = fmt.Sprintf("%s%s", base, ext)
		} else {
			name = fmt.Sprintf("%s%d%s", base, i, ext)
		}
		path := archiveDir + name
		_, err := sftpClient.Stat(path)
		if os.IsNotExist(err) {
			return path, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
	}
}

func loadHeaders(sftpClient *sftp.Client, headerPath string) ([]string, error) {
	log.Printf("[ETAPA] Tentando abrir headers: [%s]", headerPath)
	file, err := sftpClient.Open(headerPath)
	if err != nil {
		log.Printf("[ERRO] Não conseguiu abrir [%s] via SFTP: %v", headerPath, err)
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'
	headers, err := reader.Read()
	if err != nil {
		log.Printf("[ERRO] Falha lendo headers do arquivo [%s]: %v", headerPath, err)
		return nil, err
	}

	log.Printf("[OK] Headers carregados: %+v", headers)
	return headers, nil
}

func mapLine(header, record []string) map[string]string {
	row := make(map[string]string)
	for i, campo := range header {
		if i < len(record) {
			row[campo] = record[i]
		}
	}
	return row
}

func connectSFTP(cfg Config) (*sftp.Client, error) {
	config := &ssh.ClientConfig{
		User: cfg.SFTPUser,
		Auth: []ssh.AuthMethod{
			ssh.Password(cfg.SFTPPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
	addr := net.JoinHostPort(cfg.SFTPHost, cfg.SFTPPort)
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, fmt.Errorf("[ERRO] SSH dial erro: %w", err)
	}
	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, fmt.Errorf("[ERRO] SFTP client erro: %w", err)
	}
	return client, nil
}
