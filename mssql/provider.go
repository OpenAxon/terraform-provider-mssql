package mssql

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/betr-io/terraform-provider-mssql/mssql/model"
	"github.com/betr-io/terraform-provider-mssql/sql"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type mssqlProvider struct {
	factory model.ConnectorFactory
	logger  *zerolog.Logger
}

const (
	providerLogFile = "terraform-provider-mssql.log"
)

var (
	defaultTimeout = schema.DefaultTimeout(30 * time.Second)
)

func New(version, commit string) func() *schema.Provider {
	return func() *schema.Provider {
		return Provider(sql.GetFactory())
	}
}

func Provider(factory model.ConnectorFactory) *schema.Provider {
	var LoginMethods = []string{
		"login",
		"azure_login",
		"azuread_default_chain_auth",
		"azuread_managed_identity_auth",
	}

	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"debug": {
				Type:        schema.TypeBool,
				Description: fmt.Sprintf("Enable provider debug logging (logs to file %s)", providerLogFile),
				Optional:    true,
				Default:     false,
			},
			"host": {
				Type:        schema.TypeString,
				Description: "FQDN or IP address of the SQL endpoint. Can be set with MSSQL_HOSTNAME environment variable",
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("MSSQL_HOSTNAME", nil),
				DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
					return strings.EqualFold(old, new)
				},
			},
			"port": {
				Type:        schema.TypeString,
				Description: "TCP port of SQL endpoint. Defaults to 1433. Can be set with MSSQL_PORT environment variable",
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("MSSQL_PORT", DefaultPort),
			},
			"login": {
				Type:         schema.TypeSet,
				Optional:     true,
				ExactlyOneOf: LoginMethods,
				Elem: &schema.Provider{
					Schema: map[string]*schema.Schema{
						"username": {
							Type:        schema.TypeString,
							Required:    true,
							DefaultFunc: schema.EnvDefaultFunc("MSSQL_USERNAME", nil),
						},
						"password": {
							Type:        schema.TypeString,
							Required:    true,
							Sensitive:   true,
							DefaultFunc: schema.EnvDefaultFunc("MSSQL_PASSWORD", nil),
						},
					},
				},
			},
			"azure_login": {
				Type:         schema.TypeSet,
				Optional:     true,
				ExactlyOneOf: LoginMethods,
				Elem: &schema.Provider{
					Schema: map[string]*schema.Schema{
						"tenant_id": {
							Type:        schema.TypeString,
							Required:    true,
							DefaultFunc: schema.EnvDefaultFunc("MSSQL_TENANT_ID", nil),
						},
						"client_id": {
							Type:        schema.TypeString,
							Required:    true,
							DefaultFunc: schema.EnvDefaultFunc("MSSQL_CLIENT_ID", nil),
						},
						"client_secret": {
							Type:        schema.TypeString,
							Required:    true,
							Sensitive:   true,
							DefaultFunc: schema.EnvDefaultFunc("MSSQL_CLIENT_SECRET", nil),
						},
					},
				},
			},
			"azuread_default_chain_auth": {
				Type:         schema.TypeSet,
				MaxItems:     1,
				Optional:     true,
				ExactlyOneOf: LoginMethods,
				Elem:         &schema.Provider{},
			},
			"azuread_managed_identity_auth": {
				Type:         schema.TypeSet,
				Optional:     true,
				ExactlyOneOf: LoginMethods,
				Elem: &schema.Provider{
					Schema: map[string]*schema.Schema{
						"user_id": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"mssql_login": resourceLogin(),
			"mssql_user":  resourceUser(),
		},
		DataSourcesMap: map[string]*schema.Resource{},
		ConfigureContextFunc: func(ctx context.Context, data *schema.ResourceData) (interface{}, diag.Diagnostics) {
			return providerConfigure(ctx, data, factory)
		},
	}
}

func providerConfigure(ctx context.Context, data *schema.ResourceData, factory model.ConnectorFactory) (model.Provider, diag.Diagnostics) {
	isDebug := data.Get("debug").(bool)
	logger := newLogger(isDebug)

	logger.Info().Msg("Created provider")

	return mssqlProvider{factory: factory, logger: logger}, nil
}

func (p mssqlProvider) GetConnector(prefix string, data *schema.ResourceData) (interface{}, error) {
	return p.factory.GetConnector(prefix, data)
}

func (p mssqlProvider) ResourceLogger(resource, function string) zerolog.Logger {
	return p.logger.With().Str("resource", resource).Str("func", function).Logger()
}

func (p mssqlProvider) DataSourceLogger(datasource, function string) zerolog.Logger {
	return p.logger.With().Str("datasource", datasource).Str("func", function).Logger()
}

func newLogger(isDebug bool) *zerolog.Logger {
	var writer io.Writer = nil
	logLevel := zerolog.Disabled
	if isDebug {
		f, err := os.OpenFile(providerLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Err(err).Msg("error opening file")
		}
		writer = f
		logLevel = zerolog.DebugLevel
	}
	logger := zerolog.New(writer).Level(logLevel).With().Timestamp().Logger()
	return &logger
}
