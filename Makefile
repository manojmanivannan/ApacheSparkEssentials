.DEFAULT_GOAL := help

ifdef OS
        REPLACE_COMMAND=powershell -Command "(gc docker-compose.yaml) -replace 'CURRENT_PATH', '$(shell cd)' | Out-File -encoding UTF8 docker-compose1.yaml"
else
   ifeq ($(shell uname), Linux)
          REPLACE_COMMAND=sed 's*CURRENT_PATH*$(shell pwd)*' docker-compose.yaml > docker-compose1.yaml
   endif
endif

launch:
		@echo Replacing the path to $(call REPLACE_DIR)
		$(call REPLACE_COMMAND)
		@echo Starting containers
		docker-compose -f docker-compose1.yaml up

stop:
		@echo Stopping containers
		docker-compose -f docker-compose1.yaml down

help:
		@echo "launch : Launch docker containers to run notebooks"
		@echo "stop   : Stop all docker containers"