use crossbeam_channel::unbounded;
use zippy_core::ZippyError;
use zippy_openctp_core::{Ctp2rsMdDriver, MdDriver, OpenCtpMarketDataSourceConfig};

#[test]
fn ctp_driver_keeps_static_subscription_config() {
    let config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["IF2506".to_string(), "IH2506".to_string()],
        ".cache/openctp/md".to_string(),
    );

    let driver = Ctp2rsMdDriver::new(config.clone());

    assert_eq!(driver.instruments(), config.instruments.as_slice());
}

#[test]
fn ctp_driver_start_fails_when_md_dynlib_path_is_not_configured() {
    let config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["IF2506".to_string()],
        ".cache/openctp/md".to_string(),
    );
    let (tx, _rx) = unbounded();

    let error = match Box::new(Ctp2rsMdDriver::new(config)).start(tx) {
        Ok(_) => panic!("driver start should fail when md dynlib path is not configured"),
        Err(error) => error,
    };

    assert!(matches!(error, ZippyError::Io { .. }));
    assert!(error
        .to_string()
        .contains("openctp md dynlib path is not configured"));
}
