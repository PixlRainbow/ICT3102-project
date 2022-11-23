# ICT3102 Demo
Currently, this repo only contains code to demonstrate how communication between different containers is achieved.
## Deployment
### On VS Code
Right click on `docker-compose.yaml` and select "docker compose up".
### Other
From the terminal, change directory to project root folder, then run command `docker compose up --build -d`
## Test
### Using Docker Desktop
1. Click on the three dots next to the vqa or vqg container, then select "open in terminal".
2. Run command `python test_job_submit.py VQG:jobs Cyber-woman-corn-closeup.jpg`
3. Observe the logs in the other containers by clicking the three dots, then selecting "view details".
4. To view the results, go back to the container's terminal and re-run the command.
### Using Command Line
1. `docker exec -it ict3102-vqa-1 bash` (or `ict3102-vqa-1`) to enter the container's terminal.
2. Run the same command from *Using Docker Desktop* step 2.
3. Using another terminal/command prompt window, run `docker logs ict3102-vqg-1` (or `ict3102-vqa-1`) to view the log output of the respective containers.
4. Same as step 4 of *Using Docker Desktop*.
